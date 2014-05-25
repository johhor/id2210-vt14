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
    private final String TMP_FILE_NAME = "temp.tst";

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
        subscribe(handleBatchRequest, indexPort);
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
            currId = MSG_ID_START_VALUE;
            avgMemPerCpu = 0.0;
            stat = new RunTimeStatistics();
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
            
            if (rh.isBatch()) {
            	BatchRequestHandler brh = (BatchRequestHandler)rh;
            	brh.tryAddResponce(event);
            	
            	if (brh.getSelectedNodes().size()==brh.getNumMachines()) {
            		for (Address a : brh.getNodes()) {
            			RequestResources.Allocate allocate = new RequestResources.Allocate(self, a, rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),rh.getTimeCreatedAt());
            			trigger(allocate, networkPort);
            		}
            		requestResourceResponses.remove(event.getId());
            	} else if (brh.getWaitingNumResponses() == 0){
            		sendFirstSearchRequestsToNeighbour(brh.getBestResponse().getSource(),rh);
            		requestResourceResponses.remove(event.getId());
            	}
            } else {
            	RequestResources.Response best = rh.bestAndAllReceived(event);
            	if (best != null) {
            		if(best.isAvailable()){
            			RequestResources.Allocate allocate = new RequestResources.Allocate(self, best.getSource(), rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),rh.getTimeCreatedAt());
            			trigger(allocate, networkPort);
            			requestResourceResponses.remove(event.getId());
            		}else{//Find best node in gradient to ask for resources
            			sendFirstSearchRequestsToNeighbour(best.getSource(),rh);
            		}
            		requestResourceResponses.remove(event.getId());
            	}
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
                updateAvailableResourses();
            } else {
                availableResources.allocate(event.getNumCpus(), event.getAmountMemInMb());
                updateAvailableResourses();
                ScheduleTimeout st = new ScheduleTimeout(event.getTime());
                st.setTimeoutEvent(new TaskFinished(st, event.getNumCpus(), event.getAmountMemInMb()));
                trigger(st, timerPort);
                long timeToSchedule = getTimeElapsedUntilNowFrom(event.getTimeCreatedAt());
                stat.printData(TMP_FILE_NAME,timeToSchedule);
            }
        }
    };
    Handler<RequestResource> handleRequestResource = new Handler<RequestResource>() {
        @Override
        public void handle(RequestResource event) {
            //System.out.println("Allocate resources: " + event.getNumCpus() + " + " + event.getMemoryInMbs());
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
            	ArrayList<PeerDescriptor> neighbours = (useCPUGradient ? neighboursCPU : neighboursMEM);
            	int amountOfProbes = neighbours.size() > MAX_NUM_PROBES ? MAX_NUM_PROBES : neighbours.size();
            	int id = getNewMsgId();
            	requestResourceResponses.put(id, new RequestHandler(amountOfProbes, event.getNumCpus(), event.getMemoryInMbs(), event.getTimeToHoldResource(),useCPUGradient, getSystemTime()));
            	sendRequestsToRandomNeighbourSet(id);
            }
        }
    };
    Handler<BatchRequestResource> handleBatchRequest = new Handler<BatchRequestResource>() {
        @Override
        public void handle(BatchRequestResource event) {
        	//System.out.println("Machines: "+event.getNumMachines()+" CPUs: "+event.getNumCpus()+" Mem: "+event.getMemoryInMbs()+" Time: "+event.getTimeToHoldResource());
        	Long createdAt = getSystemTime();
        	boolean useCPUGradient;
            try{
                useCPUGradient = isCpuDominantResourse(event.getMemoryInMbs() , event.getNumCpus());
            }catch(ArithmeticException ae){
                useCPUGradient = false;
            }
        	ArrayList<PeerDescriptor> neighbours = (useCPUGradient ? neighboursCPU : neighboursMEM);
        	int amountOfProbes = neighbours.size() > MAX_NUM_PROBES*event.getNumMachines() ? MAX_NUM_PROBES*event.getNumMachines() : neighbours.size();
        	int id = getNewMsgId();
        	requestResourceResponses.put(id, new BatchRequestHandler(amountOfProbes*event.getNumMachines(),event, useCPUGradient,createdAt));
                
        	sendRequestsToRandomNeighbourSet(id);
        }
    };
    
    
    Handler<TManSample> handleTManSample = new Handler<TManSample>() {
        @Override
        public void handle(TManSample event) {
            int sumMEM = 0;
            int sumCPU = 0;
            //System.out.println("Received samples: " + event.getSample().size());
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
            if (!taskQueue.isEmpty()) {
                RequestResources.Allocate first = taskQueue.get(0);
                if (availableResources.allocate(first.getNumCpus(), first.getAmountMemInMb())) {
                    taskQueue.remove(0);
                    updateAvailableResourses();
                    ScheduleTimeout st = new ScheduleTimeout(first.getTime());
                    st.setTimeoutEvent(new TaskFinished(st, first.getNumCpus(), first.getAmountMemInMb()));
                    trigger(st, timerPort);
                    stat.printData(TMP_FILE_NAME,getTimeElapsedUntilNowFrom(first.getTimeCreatedAt()));
                }
            }
        }
    };
    Handler<RequestResources.RequestTimeout> handleRequestTimeout = new Handler<RequestResources.RequestTimeout>() {
        @Override
        public void handle(RequestResources.RequestTimeout e) {
            RequestHandler rh = requestResourceResponses.get(e.getId());
            if (rh != null) {
            	if (rh.isBatch()) {
                	BatchRequestHandler brh = (BatchRequestHandler)rh;
                	if (brh.getNumReceivedResponses()>0) {
                		for (Address a : brh.getNodes()) {
                			RequestResources.Allocate allocate = new RequestResources.Allocate(self, a, rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),rh.getTimeCreatedAt());
                			trigger(allocate, networkPort);
                		}
                	} else {
                		for (int i=0; i<brh.numMachines; i++) {
                			RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),rh.getTimeCreatedAt());
                			trigger(allocate, networkPort);
                		}
                	}
                	
                } else {
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
                	else {//Else we resend to another random sample of nodes
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
        	BestSearchResponse bsr = searchResponses.remove(e.getId());
            if (bsr != null) {
            	if (bsr.getRequestHandler().isBatch()) {
                	BatchRequestHandler brh = (BatchRequestHandler)bsr.getRequestHandler();
                	if (brh.getNumReceivedResponses()>0) {
                		for (Address a : brh.getNodes()) {
                			RequestResources.Allocate allocate = new RequestResources.Allocate(self, a, brh.getNumCpus(), brh.getAmountMemInMb(), brh.getTime(),brh.getTimeCreatedAt());
                			trigger(allocate, networkPort);
                		}
                	} else {
                		for (int i=0; i<brh.numMachines; i++) {
                			RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, brh.getNumCpus(), brh.getAmountMemInMb(), brh.getTime(),brh.getTimeCreatedAt());
                			trigger(allocate, networkPort);
                		}
                	}
                	
                } else {
                	if(bsr.getBestResponse()!=null){
                        //  System.out.println(" With a best responce Choosen! +++++");
                          RequestResources.Allocate a = new RequestResources.Allocate(self, bsr.getBestResponse().getSource(), bsr.getRequestHandler().getNumCpus(), bsr.getRequestHandler().getAmountMemInMb(), bsr.getRequestHandler().getTime(), bsr.getRequestHandler().getTimeCreatedAt());
                          trigger(a, networkPort);
                      }
                      else{
                       //   System.out.println(" with no responce choosen! -------");
                          //allocate on self
                    	  RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, bsr.getRequestHandler().getNumCpus(), bsr.getRequestHandler().getAmountMemInMb(), bsr.getRequestHandler().getTime(),bsr.getRequestHandler().getTimeCreatedAt());
                          trigger(allocate, networkPort);
                      }
                }
                requestResourceResponses.remove(e.getId());
            }
        }
    };
    Handler<SearchResourceMsg.Request> handleSearchRequest = new Handler<SearchResourceMsg.Request>() {
        @Override
        public void handle(SearchResourceMsg.Request event) {
            Address bestNeighbour = getBestNeighbour(event.isCpuMsg());
            SearchResourceMsg.Response response = new SearchResourceMsg.Response(self, event.getSource(),bestNeighbour,availableResources, event.getMsgId());
            trigger(response, networkPort);
        }
    };
    Handler<SearchResourceMsg.Response> handleSearchResponse = new Handler<SearchResourceMsg.Response>(){
        @Override
        public void handle(SearchResourceMsg.Response event) {
            BestSearchResponse bsr = searchResponses.remove(event.getMsgId());
            if(bsr == null){//if responce already have timed out
                return;
            }
            boolean success = event.getNodesResources().isAvailable(bsr.getRequestHandler().getNumCpus(), bsr.getRequestHandler().getAmountMemInMb()) && event.getNodesResources().getQueueLength()==0;
            if (bsr.getRequestHandler().isBatch()) {
            	BatchRequestHandler brh = (BatchRequestHandler)bsr.getRequestHandler();
            	brh.tryAddResponce(new RequestResources.Response(event.getSource(), event.getDestination(), success, event.getNodesResources().getQueueLength(), event.getMsgId()));
            	
            	if (brh.getSelectedNodes().size()==brh.getNumMachines()) {
            		for (Address a : brh.getNodes()) {
            			RequestResources.Allocate allocate = new RequestResources.Allocate(self, a, bsr.getRequestHandler().getNumCpus(), bsr.getRequestHandler().getAmountMemInMb(), bsr.getRequestHandler().getTime(),bsr.getRequestHandler().getTimeCreatedAt());
            			trigger(allocate, networkPort);
            		}
            	} else if (bsr.getBestResponse().getNodesResources().getQueueLength() > event.getNodesResources().getQueueLength()){
            		bsr.replaceBestResponse(event);
            		sendNextSearchRequestsToNeighbour(event.getNextNode(), bsr);
            	} else { //best node is reached, allocate
            		for (Address a : brh.getNodes()) {
            			RequestResources.Allocate allocate = new RequestResources.Allocate(self, a, bsr.getRequestHandler().getNumCpus(), bsr.getRequestHandler().getAmountMemInMb(), bsr.getRequestHandler().getTime(),bsr.getRequestHandler().getTimeCreatedAt());
            			trigger(allocate, networkPort);
            		}
            	}
            } else {
            	if (success) {
            		RequestResources.Allocate allocate = new RequestResources.Allocate(self, event.getSource(), bsr.getRequestHandler().getNumCpus(), bsr.getRequestHandler().getAmountMemInMb(), bsr.getRequestHandler().getTime(),bsr.getRequestHandler().getTimeCreatedAt());
            		trigger(allocate, networkPort);
            		//searchResponses.remove(responce.getMsgId());
            	}
            	//If this responce isnt getting better, we stop search and Allocate to the prev one
            	else if (bsr.getBestResponse().getNodesResources().getQueueLength() <= event.getNodesResources().getQueueLength()) {
            		RequestResources.Allocate allocate = new RequestResources.Allocate(self, bsr.getBestResponse().getSource(), bsr.getRequestHandler().getNumCpus(), bsr.getRequestHandler().getAmountMemInMb(), bsr.getRequestHandler().getTime(),bsr.getRequestHandler().getTimeCreatedAt());
            		trigger(allocate, networkPort);
                
            		// searchResponses.remove(responce.getMsgId());
            	} else if (bsr.getBestResponse().getNodesResources().getQueueLength() > event.getNodesResources().getQueueLength()) {
            		bsr.replaceBestResponse(event);
            		sendNextSearchRequestsToNeighbour(event.getNextNode(), bsr);
            	}
            }
            searchResponses.remove(event.getMsgId());
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
    private void sendRequestsToRandomNeighbourSet(int requestHandlerId) {
    	RequestHandler rh = requestResourceResponses.get(requestHandlerId);
        
        //If no neighbours we alocate it to ou self
        if (rh.getWaitingNumResponses() == 0) {
        	if (rh.isBatch()) {
        		for (int i=0; i<((BatchRequestHandler)rh).numMachines; i++) {
        			RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),rh.getTimeCreatedAt());
        			trigger(allocate, networkPort);
        		}
        	} else {
        		RequestResources.Allocate allocate = new RequestResources.Allocate(self, self, rh.getNumCpus(), rh.getAmountMemInMb(), rh.getTime(),rh.getTimeCreatedAt());
            	trigger(allocate, networkPort);
        	}
        } else {
        	ArrayList<PeerDescriptor> tempNeigh = (rh.isCPUMsg() ? neighboursCPU : neighboursMEM);
        	int amountOfProbes = (rh.getWaitingNumResponses()<=tempNeigh.size()? rh.getWaitingNumResponses() : tempNeigh.size());
        	rh.setWaitingNumResponses(amountOfProbes);
            for (int i = 0; i < amountOfProbes; i++) {
                PeerDescriptor dest = tempNeigh.remove(random.nextInt(tempNeigh.size()));
                RequestResources.Request req = new RequestResources.Request(self, dest.getAddress(), rh.getNumCpus(), rh.getAmountMemInMb(), requestHandlerId);
                trigger(req, networkPort);
            }
            ScheduleTimeout st = new ScheduleTimeout(STANDARD_TIME_OUT_DELAY);
            st.setTimeoutEvent(new RequestResources.RequestTimeout(st, requestHandlerId));
            trigger(st, timerPort);
        }
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
    		//We remove the connection between the old ID and bsr and add a new ID
            int newMsgId = getNewMsgId();
            searchResponses.put(newMsgId, bsr);
            SearchResourceMsg.Request req = new SearchResourceMsg.Request(self, dest, bsr.getRequestHandler().isCPUMsg(), newMsgId);
            trigger(req, networkPort);
            ScheduleTimeout st = new ScheduleTimeout(STANDARD_TIME_OUT_DELAY);
            st.setTimeoutEvent(new SearchResourceMsg.RequestTimeout(st, newMsgId));
            trigger(st, timerPort);
    }
    private void sendFirstSearchRequestsToNeighbour(Address dest, RequestHandler rh ) {
            Address bestNeighbour= getBestNeighbour(rh.isCPUMsg());
            //When we send away our first searchRequest, we create a dummy entry in our responces.
            int msgId = getNewMsgId();
            SearchResourceMsg.Response response = new  SearchResourceMsg.Response(self, self, bestNeighbour, availableResources, msgId);
            BestSearchResponse bsr = new BestSearchResponse(rh, response);
            searchResponses.put(msgId, bsr);    
            
            SearchResourceMsg.Request req = new SearchResourceMsg.Request(self, dest, bsr.getRequestHandler().isCPUMsg(), msgId);
            trigger(req, networkPort);
            
            ScheduleTimeout st = new ScheduleTimeout(STANDARD_TIME_OUT_DELAY);
            st.setTimeoutEvent(new SearchResourceMsg.RequestTimeout(st, msgId));
            trigger(st, timerPort);
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
}
