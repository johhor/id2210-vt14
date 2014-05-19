package resourcemanager.system.peer.rm;

import common.configuration.RmConfiguration;
import common.peer.AvailableResources;
import common.simulation.RequestResource;
import common.peer.UpdateAvailableResources;
import cyclon.system.peer.cyclon.CyclonPort;
import cyclon.system.peer.cyclon.PeerDescriptor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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
import tman.system.peer.tman.TManUpdateAvailableResourcesPort;
import cyclon.system.peer.cyclon.CyclonUpdateAvailableResourcesPort;

/**
 * Should have some comments here.
 *
 * @author jdowling
 */
public final class ResourceManager extends ComponentDefinition {

    static final int STANDARD_TIME_OUT_DELAY = 500;
    static final int MAX_NUM_PROBES = 4;
    static final int EMPTY_INDEX = Integer.MIN_VALUE;

    private static final Logger logger = LoggerFactory.getLogger(ResourceManager.class);
    Positive<RmPort> indexPort = positive(RmPort.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Negative<Web> webPort = negative(Web.class);
    Positive<TManSamplePort> tmanPort = positive(TManSamplePort.class);
    Positive<CyclonPort> cyclonPort = positive(CyclonPort.class);
    Positive<CyclonUpdateAvailableResourcesPort> cyclonUarPort = positive(CyclonUpdateAvailableResourcesPort.class);
    Positive<TManUpdateAvailableResourcesPort> tmanUarPort = positive(TManUpdateAvailableResourcesPort.class);
    ArrayList<PeerDescriptor> neighboursCPU = new ArrayList<PeerDescriptor>();
    ArrayList<PeerDescriptor> neighboursMEM = new ArrayList<PeerDescriptor>();
    ArrayList<RequestResources.Allocate> taskQueue = new ArrayList<RequestResources.Allocate>();
    
    HashMap<Integer, BestSearchResponse> searchResponses = new HashMap<Integer, BestSearchResponse>();
    //Stores respoce with smallest queue to request sent, stored by Request ID.
    HashMap<Integer, RequestHandler> requestResourceResponses = new HashMap<Integer, RequestHandler>();

    int currId;
    
    double avgMEMPerCPU = 0.0;
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
        subscribe(handleRequestResource, indexPort);
//Legacy        subscribe(handleUpdateTimeout, timerPort);
        subscribe(handleTaskFinished, timerPort);
        subscribe(handleRequestTimeout, timerPort);
        subscribe(handleResourceAllocationRequest, networkPort);
        subscribe(handleResourceAllocationResponse, networkPort);
        subscribe(handleAllocate, networkPort);
        subscribe(handleTManSample, tmanPort);
        subscribe(handleSearchRequestTimeout, timerPort);
        subscribe(handleSearchRequest, networkPort);
        subscribe(handleSearchResponse, networkPort);
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
            currId = Integer.MIN_VALUE+1;
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
            RequestHandler rh = requestResourceResponses.get(event.getId());
            if (rh == null) {
                return;
            }
            RequestResources.Response best = rh.isBestAndLastResponse(event);
            if (best != null) {
                if(best.isAvailable()){
                RequestResources.Allocate allocate = new RequestResources.Allocate(self, best.getSource(), rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime());
                trigger(allocate, networkPort);
                
                //TODO: Add Statistics
                
                requestResourceResponses.remove(event.getId());
            }else{
                sendSearchRequestsToNeighbour(best.getSource(),rh.getNumCpus(), rh.getAmountMemInMb(),rh.getTime(),rh.isCPUMsg());
            }
                requestResourceResponses.remove(event.getId());
            }
        // else If we dont get all responces we wait for timeout
        }
    };
    
    Handler<RequestResources.Allocate> handleAllocate = new Handler<RequestResources.Allocate>() {
        @Override
        public void handle(RequestResources.Allocate event) {
            boolean isAvalible = availableResources.isAvailable(event.getNumCpus(), event.getAmountMemInMb());
            if (!isAvalible) {
                taskQueue.add(event);
            } else {
                availableResources.allocate(event.getNumCpus(), event.getAmountMemInMb());
                ScheduleTimeout st = new ScheduleTimeout(event.getTime());
                st.setTimeoutEvent(new TaskFinished(st, event.getNumCpus(), event.getAmountMemInMb()));
                trigger(st, timerPort);
                availableResources.setQueueLength(taskQueue.size());
                UpdateAvailableResources uar = new UpdateAvailableResources(availableResources);
                trigger(uar, cyclonUarPort);
                trigger(uar, tmanUarPort);
            }
        }
    };
    Handler<RequestResource> handleRequestResource = new Handler<RequestResource>() {
        @Override
        public void handle(RequestResource event) {

            System.out.println("Allocate resources: " + event.getNumCpus() + " + " + event.getMemoryInMbs());
            // Ask for resources from neighbours by sending a ResourceRequest
            
            boolean useCPUGradient;
            try{
                useCPUGradient = (event.getMemoryInMbs() / event.getNumCpus()) >= avgMEMPerCPU;
            }catch(ArithmeticException ae){
                useCPUGradient = false;
            }
            sendRequestsToRandomNeighbourSet(event.getNumCpus(), event.getMemoryInMbs(), event.getTimeToHoldResource(),useCPUGradient);
        }
    };
    Handler<TManSample> handleTManSample = new Handler<TManSample>() {
        @Override
        public void handle(TManSample event) {
            int sumMEM = 0;
            int sumCPU = 0;
            System.out.println("Received samples: " + event.getSample().size());
            // receive a new list of neighbours
            if(event.isCPU()){
                neighboursCPU = replaceAllKeepOldest(event.getSample(),neighboursCPU);
            }else if(!event.isCPU()){
                neighboursMEM = replaceAllKeepOldest(event.getSample(),neighboursMEM);
            }
            for(PeerDescriptor pd : neighboursMEM){
                sumMEM += pd.getAvailableResources().getFreeMemInMbs();
                sumCPU += pd.getAvailableResources().getNumFreeCpus();
            }
            for(PeerDescriptor pd : neighboursCPU){
                sumMEM += pd.getAvailableResources().getFreeMemInMbs();
                sumCPU += pd.getAvailableResources().getNumFreeCpus();
            }
            //Sets limit of requests dominant resource
            try{
                avgMEMPerCPU = sumMEM / sumCPU;
            }catch(ArithmeticException ae){
                avgMEMPerCPU = Double.MAX_VALUE;
            }
        }
    };
    Handler<TaskFinished> handleTaskFinished = new Handler<TaskFinished>() {
        @Override
        public void handle(TaskFinished tf) {
            availableResources.release(tf.getNumCpus(), tf.getAmountMemInMb());
            if (!taskQueue.isEmpty()) {
                RequestResources.Allocate first = taskQueue.get(0);
                if (availableResources.allocate(first.getNumCpus(), first.getAmountMemInMb())) {
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
            RequestHandler rh = requestResourceResponses.get(e.getId());
            if (rh != null) {
                RequestResources.Response bestResp = rh.getBestResponse();
                if (bestResp != null) {
                    if(bestResp.isAvailable()){
                        RequestResources.Allocate allocate = new RequestResources.Allocate(self, bestResp.getSource(), rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime());
                        trigger(allocate, networkPort);
                        requestResourceResponses.remove(e.getId());
                    }
                    else{
                        sendSearchRequestsToNeighbour(bestResp.getSource(), rh.getNumCpus(), rh.getAmountMemInMb(),rh.getTime(), rh.isCPUMsg());
                    }
                } 
                else {//Else we resend to another random sample of nodes
                    boolean useCPUGradient;
                    try{
                        //IF event quote is smaller than average
                        useCPUGradient = (rh.getAmountMemInMb()/rh.getNumCpus()) >= avgMEMPerCPU;
                    } catch(ArithmeticException ae){
                        useCPUGradient = false;
                    }
                    sendRequestsToRandomNeighbourSet(rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),useCPUGradient);
                }
                requestResourceResponses.remove(e.getId());
            }
        }
    };
    //If we didnt get a responce to our search in time
    Handler<SearchResourceMsg.RequestTimeout> handleSearchRequestTimeout = new Handler<SearchResourceMsg.RequestTimeout>(){

        @Override
        public void handle(SearchResourceMsg.RequestTimeout e) {
            BestSearchResponse bsr = searchResponses.get(e.getId());
            if (bsr != null) {
            if(bsr.getBestResponse()!=null){
                RequestResources.Allocate a = new RequestResources.Allocate(self, bsr.getBestResponse().getSource(), bsr.getNumCpus(), bsr.getAmountMemInMb(), bsr.getTime());
                trigger(a, networkPort);
                searchResponses.remove(e.getId());
            }
            else{
                sendRequestsToRandomNeighbourSet(bsr.getNumCpus(), bsr.getAmountMemInMb(), bsr.getTime(),bsr.isCpuMsg());
            }
            }
        }
    };
    
    Handler<SearchResourceMsg.Request> handleSearchRequest = new Handler<SearchResourceMsg.Request>() {
        int first = 0;
        @Override
        public void handle(SearchResourceMsg.Request event) {
            Address bestNeighbour;
            if (event.isCpuMsg()) {
                bestNeighbour = neighboursCPU.get(random.nextInt(neighboursCPU.size())).getAddress();
            } else {
                bestNeighbour = neighboursMEM.get(random.nextInt(neighboursMEM.size())).getAddress();
            }
            
            SearchResourceMsg.Response response = new SearchResourceMsg.Response(self, event.getSource(),bestNeighbour,availableResources, event.getMsgId());
            trigger(response, networkPort);
        }
    };

     Handler<SearchResourceMsg.Response> handleSearchResponse = new Handler<SearchResourceMsg.Response>(){
        @Override
        public void handle(SearchResourceMsg.Response event) {
            BestSearchResponse bsr = searchResponses.get(event.getMsgId());
            
            if (event.getAskedNodesResources().isAvailable(bsr.getNumCpus(), bsr.getAmountMemInMb())) {
                RequestResources.Allocate allocate = new RequestResources.Allocate(self, event.getSource(), bsr.getNumCpus(), bsr.getAmountMemInMb(), bsr.getTime());
                trigger(allocate, networkPort);
                searchResponses.remove(event.getMsgId());
            } else if (bsr.getBestResponse().getAskedNodesResources().getQueueLength() <= event.getAskedNodesResources().getQueueLength()) {
                RequestResources.Allocate allocate = new RequestResources.Allocate(self, bsr.getBestResponse().getSource(), bsr.getNumCpus(), bsr.getAmountMemInMb(), bsr.getTime());
                trigger(allocate, networkPort);
                searchResponses.remove(event.getMsgId());
            } else if (bsr.getBestResponse().getAskedNodesResources().getQueueLength() > event.getAskedNodesResources().getQueueLength()) {
                sendSearchRequestsToNeighbour(event.getNextNode(), bsr.getNumCpus(), bsr.getAmountMemInMb(),bsr.getTime(), bsr.isCpuMsg(), event);
            }
            
        }
    };
    
     int getNextMsgId(){
         try{
             return currId++;
         }catch(Exception e){
             return currId = Integer.MIN_VALUE+1;
         }
     }
     
     private void sendRequestsToRandomNeighbourSet(int numCpus, int memoryInMb, int timeToHoldResource, boolean isCPU) {
        ArrayList<PeerDescriptor> tempNeigh = new ArrayList<PeerDescriptor>(isCPU ? neighboursCPU : neighboursMEM);
        int amountOfProbes = tempNeigh.size() > MAX_NUM_PROBES ? MAX_NUM_PROBES : tempNeigh.size();
        
        //If no neighbours we alocate it to ou self
        if (amountOfProbes == 0) {
            RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, numCpus, memoryInMb, timeToHoldResource);
            trigger(allocate, networkPort);
        } 
        
        //Else we select 4>x>0 neighbours and aske them if they can allocate the amount
        else {
            requestResourceResponses.put(currId, new RequestHandler(amountOfProbes, numCpus, memoryInMb, timeToHoldResource,isCPU));
            int id = getNextMsgId();
            for (int i = 0; i < amountOfProbes; i++) {
                PeerDescriptor dest = tempNeigh.remove(random.nextInt(tempNeigh.size()));
                RequestResources.Request req = new RequestResources.Request(self, dest.getAddress(), numCpus, memoryInMb, id);
                trigger(req, networkPort);
            }
            ScheduleTimeout st = new ScheduleTimeout(STANDARD_TIME_OUT_DELAY);
            st.setTimeoutEvent(new RequestResources.RequestTimeout(st, id));
            trigger(st, timerPort);
        }
    }
    private void sendSearchRequestsToNeighbour(Address dest,int numCpus, int memoryInMb, int timeToHoldResource, boolean isCPU){
        sendSearchRequestsToNeighbour(dest,numCpus,memoryInMb,timeToHoldResource,isCPU,EMPTY_INDEX);
    }     
    private void sendSearchRequestsToNeighbour(Address dest,int numCpus, int memoryInMb, int timeToHoldResource, boolean isCPU, int previousMsgId) {
        //If we dont have any neighbours we allocate task to our self
        if ((isCPU && neighboursCPU.size()==0) || (!isCPU && neighboursMEM.size()==0)) {
            RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, numCpus, memoryInMb, timeToHoldResource);
            trigger(allocate, networkPort);
        } 
        else {
            if (searchResponses.get(previousMsgId) != null) {
                searchResponses.put(getNextMsgId(), new BestSearchResponse(numCpus, memoryInMb, timeToHoldResource, isCPU, searchResponses.get(previousMsgId).getBestResponse()));
                searchResponses.remove(previousMsgId);
            } else {
                searchResponses.put(getNextMsgId(), new BestSearchResponse(numCpus, memoryInMb, timeToHoldResource, isCPU, null));
            }
            
            SearchResourceMsg.Request req = new SearchResourceMsg.Request(self, dest, isCPU, currId);
            trigger(req, networkPort);
            
            ScheduleTimeout st = new ScheduleTimeout(STANDARD_TIME_OUT_DELAY);
            st.setTimeoutEvent(new SearchResourceMsg.RequestTimeout(st, req.getMsgId()));
            trigger(st, timerPort);
        }
    }
    //goes through the existing and new list from TMan and returns a list with the 
    //entries from the new, unless an older version exists in the existing list.
    private ArrayList<PeerDescriptor> replaceAllKeepOldest(ArrayList<PeerDescriptor> tmanSample, ArrayList<PeerDescriptor> existing){
        ArrayList<PeerDescriptor> output = new ArrayList<PeerDescriptor>(tmanSample.size());
        
        for(PeerDescriptor pd : tmanSample){
            if(existing.contains(pd)){
                PeerDescriptor exists = existing.get(tmanSample.indexOf(pd));
                if(peerAgeComparator.compare(pd, exists) == 1){ 
                    output.add(pd);
                }
                else{
                    output.add(exists);
                } 
            }
            else{
            output.add(pd);
            }          
        } 
        return output;
    }
}
