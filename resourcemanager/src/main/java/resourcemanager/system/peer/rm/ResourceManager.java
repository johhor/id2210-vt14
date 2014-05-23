package resourcemanager.system.peer.rm;

import common.configuration.RmConfiguration;
import common.peer.AvailableResources;
import common.peer.RunTimeStatistics;
import common.simulation.RequestResource;
import common.peer.UpdateAvailableResources;
import common.simulation.BatchRequestResource;
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
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.Timeout;

/**
 * Should have some comments here.
 *
 * @author jdowling
 */
public final class ResourceManager extends ComponentDefinition {

    static final int STANDARD_TIME_OUT_DELAY = 500;
    static final int MAX_NUM_PROBES = 4;
    static final int EMPTY_INDEX = Integer.MIN_VALUE;
    static final int MSG_ID_START_VALUE = Integer.MIN_VALUE+1;

    private static final Logger logger = LoggerFactory.getLogger(ResourceManager.class);
    Positive<RmPort> indexPort = positive(RmPort.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Negative<Web> webPort = negative(Web.class);
    Positive<TManSamplePort> tmanPort = positive(TManSamplePort.class);
    Positive<CyclonPort> cyclonPort = positive(CyclonPort.class);
    Positive<CyclonUpdateAvailableResourcesPort> cyclonUarPort = positive(CyclonUpdateAvailableResourcesPort.class);
    Positive<TManUpdateAvailableResourcesPort> tmanUarPort = positive(TManUpdateAvailableResourcesPort.class);
    
    //Lists storing the nodes state
    ArrayList<PeerDescriptor> neighboursCPU = new ArrayList<PeerDescriptor>();
    ArrayList<PeerDescriptor> neighboursMEM = new ArrayList<PeerDescriptor>();
    ArrayList<RequestResources.Allocate> taskQueue = new ArrayList<RequestResources.Allocate>();
    
    HashMap<Integer, BestSearchResponse> searchResponses = new HashMap<Integer, BestSearchResponse>();
    //Stores the response with smallest queue to request sent, for each Request ID.
    HashMap<Integer, RequestHandler> requestResourceResponses = new HashMap<Integer, RequestHandler>();

    private int currId;
    
    private double avgMemPerCpu;
    private Address self;
    private RmConfiguration configuration;
    Random random;
    private AvailableResources availableResources;
    private RunTimeStatistics stat;
            
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
        subscribe(handleTaskFinished, timerPort);
        subscribe(handleRequestTimeout, timerPort);
        subscribe(handleResourceAllocationRequest, networkPort);
        subscribe(handleResourceAllocationResponse, networkPort);
        subscribe(handleAllocate, networkPort);
        subscribe(handleTManSample, tmanPort);
        subscribe(handleSearchRequestTimeout, timerPort);
        subscribe(handleSearchRequest, networkPort);
        subscribe(handleSearchResponse, networkPort);
        subscribe(handleBatchRequest, indexPort);
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
            currId = MSG_ID_START_VALUE;
            avgMemPerCpu = 0.0;
            stat = new RunTimeStatistics(self.getId());
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
            if(rh.isBatch()){
                BatchRequestHandler brh = (BatchRequestHandler)rh;
                brh.tryAddResponce(event);
                if(brh.hasGoodAllocation()){
                    for(Address a : brh.getNodes()){
                        RequestResources.Allocate allocate = new RequestResources.Allocate(self, a, brh.getNumCpus(), brh.getAmountMemInMb(), brh.getTime(),brh.getTimeCreatedAt());
                        trigger(allocate,networkPort);
                    }
                    requestResourceResponses.remove(event.getId());
                }
                else if(brh.allResponsesReceived()){
                    brh.setSearching(true);
                    sendFirstSearchRequestsToNeighbour(brh.getBestResponse().getSource(),brh);
                    requestResourceResponses.remove(event.getId());
                }
            }
            else{
                RequestResources.Response best = rh.bestAndAllReceived(event);
                if (best != null) {
                    if(best.isAvailable()){
                        RequestResources.Allocate allocate = new RequestResources.Allocate(self, best.getSource(), rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),rh.getTimeCreatedAt());
                        trigger(allocate, networkPort);
                        requestResourceResponses.remove(event.getId());
                    }
                    else{//Find best node in gradient to ask for resources
                        sendFirstSearchRequestsToNeighbour(best.getSource(),rh);
                    }
                    requestResourceResponses.remove(event.getId());
                }
            // else If we dont get all responces we wait for timeout
            }
        }
    };
    Handler<RequestResources.Allocate> handleAllocate = new Handler<RequestResources.Allocate>() {
        @Override
        public void handle(RequestResources.Allocate event) {
            boolean isAvalible = availableResources.isAvailable(event.getNumCpus(), event.getAmountMemInMb());
            if (!isAvalible) {
                taskQueue.add(event);
                updateAvailableResourses();
            } else {
                availableResources.allocate(event.getNumCpus(), event.getAmountMemInMb());
                updateAvailableResourses();
                ScheduleTimeout st = new ScheduleTimeout(event.getTime());
                st.setTimeoutEvent(new TaskFinished(st, event.getNumCpus(), event.getAmountMemInMb()));
                trigger(st, timerPort);
                long timeToSchedule = getTimeElapsedUntilNowFrom(event.getTimeCreatedAt());
                stat.addAllocationTime(timeToSchedule);
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
                useCPUGradient = isCpuDominantResourse(event.getMemoryInMbs() , event.getNumCpus());
            }catch(ArithmeticException ae){
                useCPUGradient = false;
            }
            if (availableResources.isAvailable(event.getNumCpus(), event.getMemoryInMbs())) {
            	RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, event.getNumCpus(), event.getMemoryInMbs(), event.getTimeToHoldResource(),getSystemTime());
                trigger(allocate, networkPort);
            } else {
            	sendRequestsToRandomNeighbourSet(event.getNumCpus(), event.getMemoryInMbs(), event.getTimeToHoldResource(),useCPUGradient,getSystemTime());
            }
        }
    };
    Handler<BatchRequestResource> handleBatchRequest = new Handler<BatchRequestResource>() {
        @Override
        public void handle(BatchRequestResource e) {
            Debuggln("Batch Request: Machines "+e.getNumMachines()+" CPUs"+e.getNumCpus()+" Mem: "+e.getMemoryInMbs()+" allocTime:"+e.getTimeToHoldResource());
            boolean isCpu = isCpuDominantResourse(e.getMemoryInMbs(),e.getNumCpus());
            int msgID = getNewMsgId();
            ArrayList<PeerDescriptor> tempNeigh = new ArrayList<PeerDescriptor>(isCpu ? neighboursCPU : neighboursMEM);
            int numRequests = getAmountOfProbes(tempNeigh.size())*e.getNumMachines();
            requestResourceResponses.put(msgID, new BatchRequestHandler(numRequests, e, isCpu, getSystemTime(), msgID));
           
            for (int j = 0; j < numRequests; ) {
                for (int i = 0; i < e.getNumMachines(); i++, j++) {
                    sendRequestsToRandomNeighbourRound(msgID,tempNeigh,e.getNumCpus(), e.getMemoryInMbs(), e.getTimeToHoldResource(), isCpu,getSystemTime());
                if(j >= numRequests)
                    break;
                }
                if(j < numRequests)//This isnt needed the last lap
                    tempNeigh = new ArrayList<PeerDescriptor>(isCpu ? neighboursCPU : neighboursMEM);
            }            
            if(numRequests > 0){
                ScheduleTimeout st = new ScheduleTimeout(STANDARD_TIME_OUT_DELAY);
                st.setTimeoutEvent(new RequestResources.RequestTimeout(st, msgID));
                trigger(st, timerPort);
            }
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
                neighboursCPU = replaceAllKeepNewest(event.getSample(),neighboursCPU);
            }else if(!event.isCPU()){
                neighboursMEM = replaceAllKeepNewest(event.getSample(),neighboursMEM);
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
                avgMemPerCpu = sumMEM / sumCPU;
            }catch(ArithmeticException ae){
                avgMemPerCpu = Double.MAX_VALUE;
            }
        }
    };
    Handler<TaskFinished> handleTaskFinished = new Handler<TaskFinished>() {
        @Override
        public void handle(TaskFinished tf) {
            availableResources.release(tf.getNumCpus(), tf.getAmountMemInMb());
            while (!taskQueue.isEmpty()) {
                RequestResources.Allocate first = taskQueue.get(0);
                if (availableResources.allocate(first.getNumCpus(), first.getAmountMemInMb())) {
                    taskQueue.remove(0);
                    updateAvailableResourses();
                    ScheduleTimeout st = new ScheduleTimeout(first.getTime());
                    st.setTimeoutEvent(new TaskFinished(st, first.getNumCpus(), first.getAmountMemInMb()));
                    trigger(st, timerPort);
                    stat.addAllocationTime(getTimeElapsedUntilNowFrom(first.getTimeCreatedAt()));
                } else {
                	break;
                }
            }
        }
    };
    Handler<RequestResources.RequestTimeout> handleRequestTimeout = new Handler<RequestResources.RequestTimeout>() {
        @Override
        public void handle(RequestResources.RequestTimeout e) {
            RequestHandler rh = requestResourceResponses.get(e.getId());
            if (rh != null) {
                if(rh.isBatch()){
                    BatchRequestHandler brh = (BatchRequestHandler) rh;
                    if(brh.isSearching()){
                        return; // let the searchTimeout handle it
                    }
                    else{
                        brh.setSearching(true);
                        if (brh.getBestResponse() != null) {
                        	sendFirstSearchRequestsToNeighbour(brh.getBestResponse().getSource(),brh);
                        }
                        else //allocate on self
                        {
                        	for (int i=0; i<brh.numMachines; i++) {
                        		RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),rh.getTimeCreatedAt());
                                trigger(allocate, networkPort);
                        	}
                        }
                    }
                }else {
                RequestResources.Response bestResp = rh.getBestResponse();
                if (bestResp != null) {
                    if(bestResp.isAvailable()){
                        RequestResources.Allocate allocate = new RequestResources.Allocate(self, bestResp.getSource(), rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),rh.getTimeCreatedAt());
                        trigger(allocate, networkPort);
                    }
                    else{                        
                        sendFirstSearchRequestsToNeighbour(bestResp.getSource(), rh);
                    }
                } 
                else {//Else we allocate on self
                	RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),rh.getTimeCreatedAt());
                    trigger(allocate, networkPort);
                }
                
                }
                requestResourceResponses.remove(e.getId());
            }
        }
    };
    //If we didnt get a responce to our search in time
    Handler<SearchResourceMsg.RequestTimeout> handleSearchRequestTimeout = new Handler<SearchResourceMsg.RequestTimeout>(){
        @Override
        public void handle(SearchResourceMsg.RequestTimeout e) {
            //System.out.print("SEARCH REQ w ID "+ e.getId()+ " Just timed out!");
            BestSearchResponse bsr = searchResponses.remove(e.getId());
            if (bsr != null) {
                if(bsr.isBatchSearch()){
                    for(Address a : bsr.getBRH().getNodes()){
                        RequestResources.Allocate al = new RequestResources.Allocate(self, a, bsr.getNumCpus(), bsr.getAmountMemInMb(), bsr.getTime(), bsr.getTimeCreatedAt());
                        trigger(al, networkPort);
                    }
                    searchResponses.remove(e.getId());
                }
                else if(bsr.getBestResponse()!=null){
                  //  System.out.println(" With a best responce Choosen! +++++");
                    RequestResources.Allocate a = new RequestResources.Allocate(self, bsr.getBestResponse().getSource(), bsr.getNumCpus(), bsr.getAmountMemInMb(), bsr.getTime(), bsr.getTimeCreatedAt());
                    trigger(a, networkPort);
                }
                else{
                 //   System.out.println(" with no responce choosen! -------");
                    //Resend the messdage to random neigbours
                    sendRequestsToRandomNeighbourSet(bsr.getNumCpus(), bsr.getAmountMemInMb(), bsr.getTime(),bsr.isCpuMsg(),bsr.getTimeCreatedAt());
                }
            }
            else{ 
               // System.out.println(" But bsr was null already");
            }
        }
    };
    Handler<SearchResourceMsg.Request> handleSearchRequest = new Handler<SearchResourceMsg.Request>() {
        int first = 0;
        @Override
        public void handle(SearchResourceMsg.Request event) {
            Address bestNeighbour = getBestNeighbour(event.isCpuMsg());
            SearchResourceMsg.Response response = new SearchResourceMsg.Response(self, event.getSource(),bestNeighbour,availableResources, event.getMsgId());
            trigger(response, networkPort);
        }
    };
    Handler<SearchResourceMsg.Response> handleSearchResponse = new Handler<SearchResourceMsg.Response>(){
        @Override
        public void handle(SearchResourceMsg.Response resp) {
//            System.out.println("Received search responce with id: "+responce.getMsgId());
//            for(BestSearchResponse b :searchResponses.values()){
//                System.out.println("searchResponces in HashMap is: "+b.getBestResponse().getMsgId());
//            }
            BestSearchResponse bsr = searchResponses.remove(resp.getMsgId());
            if(bsr == null){//if responce already have timed out
                return;
            }
            //If we get an avalible node from searching
            if (resp.getNodesResources().isAvailable(bsr.getNumCpus(), bsr.getAmountMemInMb())) {
                if(bsr.isBatchSearch()){
                    //If it was a search for a batch request, we let the requestResource.Responce handle handle it
                    boolean isAvalible = bsr.getNumCpus()>=resp.getNodesResources().getNumFreeCpus()&& bsr.getAmountMemInMb()>= resp.getNodesResources().getFreeMemInMbs()&&resp.getNodesResources().getQueueLength()==0;
                    RequestResources.Response r = new RequestResources.Response(resp.getSource(),self,isAvalible, resp.getNodesResources().getQueueLength(), bsr.getBRH().getId());
                    trigger(r, networkPort);
                }
                else{    
                    RequestResources.Allocate allocate = new RequestResources.Allocate(self, resp.getSource(), bsr.getNumCpus(), bsr.getAmountMemInMb(), bsr.getTime(),bsr.getTimeCreatedAt());
                    trigger(allocate, networkPort);
                }
                searchResponses.remove(resp.getMsgId());
            }
            //If this responce isnt better
            else if (bsr.getBestResponse().getNodesResources().getQueueLength() <= resp.getNodesResources().getQueueLength()) {
                if(bsr.isBatchSearch()){
                    //If it was a search for a batchRequest
                    for(Address a : bsr.getBRH().getNodes()){
                        RequestResources.Allocate allocate = new RequestResources.Allocate(self, a,bsr.getNumCpus(), bsr.getAmountMemInMb(), bsr.getTime(),bsr.getTimeCreatedAt());
                        trigger(allocate, networkPort);
                    }
                    requestResourceResponses.remove(bsr.getBRH().getId());
                }
                //If it's a normal search we stop search and Allocate to the prev one
                else{
                    RequestResources.Allocate allocate = new RequestResources.Allocate(self, bsr.getBestResponse().getSource(), bsr.getNumCpus(), bsr.getAmountMemInMb(), bsr.getTime(),bsr.getTimeCreatedAt());
                    trigger(allocate, networkPort);
                }
                searchResponses.remove(resp.getMsgId());
            } else if (bsr.getBestResponse().getNodesResources().getQueueLength() > resp.getNodesResources().getQueueLength()) {
                bsr.replaceBestResponse(resp);
                sendNextSearchRequestsToNeighbour(resp.getNextNode(), bsr);
            }
        }
    };
    private boolean isCpuDominantResourse(int mem, int cpu){
        return (mem / cpu) < avgMemPerCpu;
    }
    private int getNewMsgId(){
         try{
             return currId++;
         }catch(Exception e){
             return currId = MSG_ID_START_VALUE;
         }
     }
    private void sendRequestsToRandomNeighbourSet(int numCpus, int memoryInMb, int timeToHoldResource, boolean isCPU, long startTime){
        int msgID = getNewMsgId();
        ArrayList<PeerDescriptor> tempNeigh = new ArrayList<PeerDescriptor>(isCPU ? neighboursCPU : neighboursMEM);
        sendRequestsToRandomNeighbourRound(msgID,tempNeigh,numCpus, memoryInMb, timeToHoldResource, isCPU,startTime);
        int amountOfProbes = getAmountOfProbes(tempNeigh.size());
        if(amountOfProbes>0){
            requestResourceResponses.put(msgID, new RequestHandler(amountOfProbes, numCpus, memoryInMb, timeToHoldResource,isCPU, startTime));
            ScheduleTimeout st = new ScheduleTimeout(STANDARD_TIME_OUT_DELAY);
            st.setTimeoutEvent(new RequestResources.RequestTimeout(st, msgID));
            trigger(st, timerPort);
        }
    }
    private void sendRequestsToRandomNeighbourRound(int msgID, ArrayList<PeerDescriptor> tempNeigh,int numCpus, int memoryInMb, int timeToHoldResource, boolean isCPU, long startTime) {
        int amountOfProbes = getAmountOfProbes(tempNeigh.size());
        //If no neighbours we alocate it to ou self
        if (amountOfProbes == 0) {
            RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, numCpus, memoryInMb, timeToHoldResource, startTime);
            trigger(allocate, networkPort);
        }
        //Else we select 4>=x>0 neighbours and aske them if they can allocate the amount
        else {
            for (int i = 0; i < amountOfProbes; i++) {
                PeerDescriptor dest = tempNeigh.remove(random.nextInt(tempNeigh.size()));
                RequestResources.Request req = new RequestResources.Request(self, dest.getAddress(), numCpus, memoryInMb, msgID);
                trigger(req, networkPort);
            }
        }
    }
    private int getAmountOfProbes(int neighSize){
        return neighSize > MAX_NUM_PROBES ? MAX_NUM_PROBES : neighSize;
    }
    private boolean isAlone(boolean isCPU){
        return (isCPU&& neighboursCPU.isEmpty()) || (!isCPU && neighboursMEM.isEmpty());
    }
    
    private void updateAvailableResourses(){
        availableResources.setQueueLength(taskQueue.size());
        UpdateAvailableResources uar = new UpdateAvailableResources(availableResources);
        trigger(uar, cyclonUarPort);
        trigger(uar, tmanUarPort);
    }
    
    private void sendNextSearchRequestsToNeighbour(Address dest,BestSearchResponse bsr){
        if (isAlone(bsr.isCpuMsg())) {
            RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, bsr.getNumCpus(), bsr.getAmountMemInMb(), bsr.getTime(),bsr.getTimeCreatedAt());
            trigger(allocate, networkPort);
        }
        else{//We remove the connection between the old ID and bsr and add a new ID
            int newMsgId = getNewMsgId();
            searchResponses.put(newMsgId, bsr);
            SearchResourceMsg.Request req = new SearchResourceMsg.Request(self, dest, bsr.isCpuMsg(), newMsgId);
            trigger(req, networkPort);
            ScheduleTimeout st = new ScheduleTimeout(STANDARD_TIME_OUT_DELAY);
            st.setTimeoutEvent(new SearchResourceMsg.RequestTimeout(st, newMsgId));
            trigger(st, timerPort);
        }
    }
    private void sendFirstSearchRequestsToNeighbour(Address dest, RequestHandler rh ) {
        //If we dont have any neighbours we allocate task to our self
        if (isAlone(rh.isCPUMsg())) {
            RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),rh.getTimeCreatedAt());
            trigger(allocate, networkPort);
        } 
        else {
            Address bestNeighbour= getBestNeighbour(rh.isCPUMsg());
            //When we send away our first searchRequest, we create a dummy entry in our responces.
            int msgId = getNewMsgId();
            SearchResourceMsg.Response response = new  SearchResourceMsg.Response(self, self, bestNeighbour, availableResources, msgId);
            BestSearchResponse bsr = new BestSearchResponse(rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(), rh.isCPUMsg(), response, rh.getTimeCreatedAt());
            searchResponses.put(msgId, bsr);    
            
            SearchResourceMsg.Request req = new SearchResourceMsg.Request(self, dest, bsr.isCpuMsg(), msgId);
            trigger(req, networkPort);
            
            ScheduleTimeout st = new ScheduleTimeout(STANDARD_TIME_OUT_DELAY);
            st.setTimeoutEvent(new SearchResourceMsg.RequestTimeout(st, req.getMsgId()));
            trigger(st, timerPort);
        }
    }
    private Address getBestNeighbour(boolean isCPU){
        Address bestNeighbour;
        if (isCPU && !neighboursCPU.isEmpty()) {
            bestNeighbour = neighboursCPU.get(0).getAddress();
        } else if(isCPU && !neighboursMEM.isEmpty()){
            bestNeighbour = neighboursMEM.get(0).getAddress();
        } else{
            bestNeighbour = self;
        }
        return bestNeighbour;
    }
    //goes through the existing and new list from TMan and returns a list with the 
    //entries from the new, unless an newer version exists in the existing list.
    private ArrayList<PeerDescriptor> replaceAllKeepNewest(ArrayList<PeerDescriptor> tmanSample, ArrayList<PeerDescriptor> existing){
        ArrayList<PeerDescriptor> output = new ArrayList<PeerDescriptor>(tmanSample.size());
        
        for(PeerDescriptor pd : tmanSample){
            if(existing.contains(pd)){
                PeerDescriptor exists = existing.get(existing.indexOf(pd));
                if(peerAgeComparator.compare(pd, exists) == 1){ 
                    output.add(exists);
                }
                else{
                    output.add(pd);
                } 
            }
            else{
            output.add(pd);
            }          
        } 
        return output;
    }
    
    private long getSystemTime(){
        return System.currentTimeMillis();
    }
    private long getTimeElapsedUntilNowFrom(long from){
        return  System.currentTimeMillis()-from;
    }
    private void Debugg(String s){
        System.err.print(s);
    }
    private void Debuggln(String s){
        System.err.println(s);
    }
}
