package resourcemanager.system.peer.rm;

import common.configuration.RmConfiguration;
import common.peer.AvailableResources;
import common.simulation.RequestResource;
import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import cyclon.system.peer.cyclon.PeerDescriptor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.web.Web;
import system.peer.RmPort;
import tman.system.peer.tman.TManSample;
import tman.system.peer.tman.TManSamplePort;

/**
 * Should have some comments here.
 *
 * @author jdowling
 */
public final class ResourceManager extends ComponentDefinition {
    
    static final int STANDARD_TIME_OUT_DELAY = 500;
    static final int MAX_NUM_PROBES = 4;

    private static final Logger logger = LoggerFactory.getLogger(ResourceManager.class);
    Positive<RmPort> indexPort = positive(RmPort.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Negative<Web> webPort = negative(Web.class);
    Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
    Positive<TManSamplePort> tmanPort = positive(TManSamplePort.class);
    ArrayList<Address> neighbours = new ArrayList<Address>();
 
    ArrayList<RequestResources.Allocate> taskQueue = new ArrayList<RequestResources.Allocate>();
    //Stores respoce with smallest queue to request sent, stored by Request ID.
    HashMap<Integer,RequestHandler> responses = new HashMap<Integer,RequestHandler>(); 
    
    int currId;
    private Address self;
    private RmConfiguration configuration;
    Random random;
    private AvailableResources availableResources;
    Comparator<PeerDescriptor> peerAgeComparator = new Comparator<PeerDescriptor>() {
        @Override
        public int compare(PeerDescriptor t, PeerDescriptor t1) {
            if (t.getAge() > t1.getAge()) {
                return 1;
            } else {
                return -1;
            }
        }
    };

	
    public ResourceManager() {

        subscribe(handleInit, control);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleRequestResource, indexPort);
        subscribe(handleUpdateTimeout, timerPort);
        subscribe(handleTaskFinished, timerPort);
        subscribe(handleResourceAllocationRequest, networkPort);
        subscribe(handleResourceAllocationResponse, networkPort);
        subscribe(handleAllocate, networkPort);
        subscribe(handleTManSample, tmanPort);
    }
	
    Handler<RmInit> handleInit = new Handler<RmInit>() {
        @Override
        public void handle(RmInit init) {
            self = init.getSelf();
            configuration = init.getConfiguration();
            random = new Random(init.getConfiguration().getSeed());
            availableResources = init.getAvailableResources();
            long period = configuration.getPeriod();
            availableResources = init.getAvailableResources();
            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period, period);
            rst.setTimeoutEvent(new UpdateTimeout(rst));
            trigger(rst, timerPort);
            currId = 0;
    }
};

    Handler<UpdateTimeout> handleUpdateTimeout = new Handler<UpdateTimeout>() {
        @Override
        public void handle(UpdateTimeout event) {

            // pick a random neighbour to ask for index updates from. 
            // You can change this policy if you want to.
            // Maybe a gradient neighbour who is closer to the leader?
            if (neighbours.isEmpty()) {
                return;
            }
            Address dest = neighbours.get(random.nextInt(neighbours.size()));
            
            
        }
    };

    Handler<RequestResources.Request> handleResourceAllocationRequest = new Handler<RequestResources.Request>() {
        @Override
        public void handle(RequestResources.Request event) {
            boolean isAvalible = availableResources.isAvailable(event.getNumCpus(), event.getAmountMemInMb());
            RequestResources.Response response = new RequestResources.Response(self, event.getSource(), isAvalible, taskQueue.size(), event.getId());
            trigger(response, networkPort); 
        }
    };
    Handler<RequestResources.Response> handleResourceAllocationResponse = new Handler<RequestResources.Response>() {
        @Override
        public void handle(RequestResources.Response event) {
            RequestHandler rh = responses.get(event.getId());
            if (rh == null)
                return;
            RequestResources.Response best = rh.isBestResponse(event);
            if (best != null) {
                RequestResources.Allocate allocate = new RequestResources.Allocate(self, best.getSource(), rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime());
                trigger(allocate, networkPort);
                responses.remove(event.getId());
            }
        }
    };
    
    Handler<RequestResources.Allocate> handleAllocate = new Handler<RequestResources.Allocate>() {

        @Override
        public void handle(RequestResources.Allocate event) {
            boolean isAvalible = availableResources.isAvailable(event.getNumCpus(), event.getAmountMemInMb());
           if(!isAvalible)
                taskQueue.add(event);
           else{
               availableResources.allocate(event.getNumCpus(), event.getAmountMemInMb());
               ScheduleTimeout st = new ScheduleTimeout(event.getTime());
               st.setTimeoutEvent(new TaskFinished(st, event.getNumCpus(), event.getAmountMemInMb()));
               trigger(st, timerPort);
           }
        }
    };
    
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            System.out.println("Received samples: " + event.getSample().size());
            
            // receive a new list of neighbours
            neighbours.clear();
            neighbours.addAll(event.getSample());

        }
    };
	
    Handler<RequestResource> handleRequestResource = new Handler<RequestResource>() {
        @Override
        public void handle(RequestResource event) {
            
            System.out.println("Allocate resources: " + event.getNumCpus() + " + " + event.getMemoryInMbs());
            // TODO: Ask for resources from neighbours
            // by sending a ResourceRequest
            sendRequestsToNeighbours(event.getNumCpus(), event.getMemoryInMbs(), event.getTimeToHoldResource());
        }
    };
    Handler<TManSample> handleTManSample = new Handler<TManSample>() {
        @Override
        public void handle(TManSample event) {
            // TODO: 
        }
    };
    Handler<TaskFinished> handleTaskFinished = new Handler<TaskFinished>() {
        @Override
        public void handle(TaskFinished tf) {
            availableResources.release(tf.getNumCpus(), tf.getAmountMemInMb());
            
            if (!taskQueue.isEmpty()){
                 RequestResources.Allocate first = taskQueue.get(0);
                if(availableResources.allocate(first.getNumCpus(),first.getAmountMemInMb())){
                    taskQueue.remove(0);
                    ScheduleTimeout st = new ScheduleTimeout(first.getTime());
                    st.setTimeoutEvent(new TaskFinished(st, first.getNumCpus(), first.getAmountMemInMb()));
                    trigger(st, timerPort);
                } 
            }
        }
    };
    Handler<RequestResources.RequestTimeout> handleRequestTimeout = new Handler<RequestResources.RequestTimeout>() {

        @Override
        public void handle(RequestResources.RequestTimeout e) {
            RequestHandler rh = responses.get(e.getId());
            RequestResources.Response bestResp = rh.getBestResponse();
            if (bestResp != null) {
                RequestResources.Allocate allocate = new RequestResources.Allocate(self, bestResp.getSource(), rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime());
                trigger(allocate, networkPort);
            } else {
                sendRequestsToNeighbours(rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime());
            }
            responses.remove(e.getId());
        }
    };
    private void sendRequestsToNeighbours(int numCpus, int memoryInMb, int timeToHoldResource) {
        ArrayList<Address> tempNeigh = new ArrayList<Address>(neighbours);
            int amountOfProbes = tempNeigh.size() > MAX_NUM_PROBES ? MAX_NUM_PROBES : neighbours.size();
            if (amountOfProbes == 0) {
                RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, numCpus, memoryInMb, timeToHoldResource);
                trigger(allocate, networkPort);
            } else {
            responses.put(currId, new RequestHandler(amountOfProbes, numCpus, memoryInMb, timeToHoldResource));
            for (int i = 0; i < amountOfProbes; i++) {
                Address dest = tempNeigh.remove(random.nextInt(neighbours.size()));
                RequestResources.Request req = new RequestResources.Request(self, dest, numCpus, memoryInMb,currId);
                trigger(req, networkPort);
            }
            ScheduleTimeout st = new ScheduleTimeout(STANDARD_TIME_OUT_DELAY);
            st.setTimeoutEvent(new RequestResources.RequestTimeout(st, currId));
            trigger(st, timerPort);
            currId++;
            }
    }
            
}