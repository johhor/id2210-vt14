package resourcemanager.system.peer.rm;

import common.configuration.RmConfiguration;
import common.peer.AvailableResources;
import common.peer.RunTimeStatistics;
import common.simulation.RequestResource;
import common.simulation.BatchRequestResource;
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
    RunTimeStatistics stat;
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
        subscribe(handleBatchRequestResource, indexPort);
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
            stat = new RunTimeStatistics(self.getId());
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
            if (rh == null){
                return;
        	}
            if(!rh.isBatch()){
            	RequestResources.Response best = rh.isBestResponse(event);
            	if (best != null) {
            		RequestResources.Allocate allocate = new RequestResources.Allocate(self, best.getSource(), rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),rh.getTimeCreatedAt());
            		trigger(allocate, networkPort);
            		responses.remove(event.getId());
            	}
            }else if(rh.isBatch()){
            	BatchRequestHandler brh = (BatchRequestHandler)rh;
            	brh.tryAddResponce(event);
            	if(brh.hasGoodAllocation()||brh.allResponsesReceived()){
            		for(Address a : brh.getNodes()){
            			RequestResources.Allocate allocate = new RequestResources.Allocate(self, a, brh.getNumCpus(), brh.getAmountMemInMb(), brh.getTime(),brh.getTimeCreatedAt());
            			trigger(allocate, networkPort);
            		}
                    responses.remove(event.getId());
            	}
            }
        }
    };
    
    Handler<RequestResources.Allocate> handleAllocate = new Handler<RequestResources.Allocate>() {

        @Override
        public void handle(RequestResources.Allocate event) {
            boolean isAvalible = availableResources.isAvailable(event.getNumCpus(), event.getAmountMemInMb());
           if(!isAvalible || !taskQueue.isEmpty())
                taskQueue.add(event);
           else{
               availableResources.allocate(event.getNumCpus(), event.getAmountMemInMb());
               ScheduleTimeout st = new ScheduleTimeout(event.getTime());
               st.setTimeoutEvent(new TaskFinished(st, event.getNumCpus(), event.getAmountMemInMb()));
               trigger(st, timerPort);
               stat.addAllocationTime(getTimeElapsedSince(event.getTimeCreatedAt()));
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
            sendRequestsToNeighbours(event.getNumCpus(), event.getMemoryInMbs(), event.getTimeToHoldResource(),getSystemTime());
        }
    };
    
    Handler<BatchRequestResource> handleBatchRequestResource = new Handler<BatchRequestResource>() {
        @Override
        public void handle(BatchRequestResource event) {
        	System.out.println("Batch request: "+event.getNumMachines()+" machines, each with "+event.getMemoryInMbs()+
        			"amount of memory and "+event.getNumCpus()+" cores each, time: "+event.getTimeToHoldResource());
        	
        	int numRequests = getAmountOfProbes(neighbours.size()) * event.getNumMachines();
        	responses.put(currId, new BatchRequestHandler(numRequests,event,getSystemTime()));
        	
        	//We use a double for loop to guarantee that we ask AMOUNT_OF_PROBES times for each nodes
        	for(int j = 0; j < numRequests;){
        		ArrayList<Address> tempNeigh = new ArrayList<Address>(neighbours);
        		for (int i=0; i<event.getNumMachines(); i++, j++) {
        			sendRequestsToNeighboursRound(tempNeigh,event.getNumCpus(), event.getMemoryInMbs(), event.getTimeToHoldResource(),getSystemTime());
        			if(j >= numRequests)
        				break;
        		}
        	}
            ScheduleTimeout st = new ScheduleTimeout(STANDARD_TIME_OUT_DELAY);
            st.setTimeoutEvent(new RequestResources.RequestTimeout(st, currId));
            trigger(st, timerPort);
            currId++;
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
                    stat.addAllocationTime(getTimeElapsedSince(first.getTimeCreatedAt()));
                } 
            }
        }
    };
    Handler<RequestResources.RequestTimeout> handleRequestTimeout = new Handler<RequestResources.RequestTimeout>() {

        @Override
        public void handle(RequestResources.RequestTimeout e) {
            RequestHandler rh = responses.get(e.getId());
            if(rh == null)
            	return;
            if(rh.isBatch()){
            	BatchRequestHandler brh = (BatchRequestHandler)rh;
                if(brh.getNumReceivedResponses()>0){
                    for(Address a : brh.getNodes()){
                        RequestResources.Allocate allocate = new RequestResources.Allocate(self, a, brh.getNumCpus(), brh.getAmountMemInMb(), brh.getTime(),brh.getTimeCreatedAt());
                        trigger(allocate, networkPort);
                    }
                }
                else{
                    //Worst case is that we dont have any responces, in which case we allocate it all on our self
                    RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, brh.getNumCpus(), brh.getAmountMemInMb(), brh.getTime(),brh.getTimeCreatedAt());
                    for (int i = 0; i < brh.getNumMachines; i++) {
                        trigger(allocate, networkPort);
                    }
                }
                responses.remove(e.getId());
            }
            else{
            	RequestResources.Response bestResp = rh.getBestResponse();
            	if (bestResp != null) {
                    RequestResources.Allocate allocate = new RequestResources.Allocate(self, bestResp.getSource(), rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),rh.getTimeCreatedAt());
                    trigger(allocate, networkPort);
            	} 
            	else {
                    sendRequestsToNeighbours(rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),rh.getTimeCreatedAt());
            	}
            	responses.remove(e.getId());
            }
        }
    };
    private void sendRequestsToNeighbours(int numCpus, int memoryInMb, int timeToHoldResource,long requestedAt) {
    	ArrayList<Address> tempNeigh = new ArrayList<Address>(neighbours);
    	int amountOfProbes = getAmountOfProbes(tempNeigh.size());
    	responses.put(currId, new RequestHandler(amountOfProbes, numCpus, memoryInMb, timeToHoldResource,requestedAt));
    	sendRequestsToNeighboursRound(tempNeigh,numCpus, memoryInMb, timeToHoldResource,requestedAt);
    	if(getAmountOfProbes(tempNeigh.size())>0){
            ScheduleTimeout st = new ScheduleTimeout(STANDARD_TIME_OUT_DELAY);
            st.setTimeoutEvent(new RequestResources.RequestTimeout(st, currId));
            trigger(st, timerPort);
    		currId++;
    	}
    }
    private void sendRequestsToNeighboursRound(ArrayList<Address> tempNeigh,int numCpus, int memoryInMb, int timeToHoldResource,long requestedAt) {
        
        int amountOfProbes = getAmountOfProbes(tempNeigh.size());
        if (amountOfProbes == 0) {
                RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, numCpus, memoryInMb, timeToHoldResource,requestedAt);
                trigger(allocate, networkPort);
        } 
        else {
            for (int i = 0; i < amountOfProbes; i++) {
                Address dest = tempNeigh.remove(random.nextInt(tempNeigh.size()));
                RequestResources.Request req = new RequestResources.Request(self, dest, numCpus, memoryInMb,currId);
                trigger(req, networkPort);
            }
        }
    }
	private int getAmountOfProbes(int neighbours){
		return neighbours > MAX_NUM_PROBES ? MAX_NUM_PROBES : neighbours;
	}
    private long getSystemTime(){
    	return System.currentTimeMillis();
    }
    private long getTimeElapsedSince(long since){
    	return System.currentTimeMillis() - since;
    }       
}