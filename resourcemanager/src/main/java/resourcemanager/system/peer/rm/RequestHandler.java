/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package resourcemanager.system.peer.rm;

/**
 *
 * @author Linus
 */
public class RequestHandler {
    private final int numCpus;
    private final int amountMemInMb;
    private final int time;
    private final boolean isCPUMsg;
    private int waitingNumRes;
    private long timeCreatedAt;
    
    RequestResources.Response bestResponse;
    
    public RequestHandler(int waitingNumRes, int numCpus, int amountMemInMb, int time, boolean isCPU, long startTime) {
        this.waitingNumRes = waitingNumRes;
        this.numCpus = numCpus;
        this.amountMemInMb = amountMemInMb;
        this.time =time;
        isCPUMsg = isCPU;
        timeCreatedAt = startTime;
    }
    
    public RequestResources.Response bestAndAllReceived(RequestResources.Response response) {
        waitingNumRes--;
        if (bestResponse == null) {
            bestResponse = response;
        } else if (!bestResponse.isAvailable() && response.isAvailable()) {
            bestResponse = response;
        } else if (bestResponse.getQueueSize()>response.getQueueSize()) {
            bestResponse = response;
        }
        
        if (waitingNumRes == 0)
            return bestResponse;
        return null;
    }
    public boolean isBatch(){
	return false;
    }
    public RequestResources.Response getBestResponse() {
        return bestResponse;
    }
    public int getNumCpus() {
        return numCpus;
    }
    public int getAmountMemInMb() {
        return amountMemInMb;
    }
    public int getTime() {
        return time;
    }
    public boolean isCPUMsg() {
        return isCPUMsg;
    }
    public long getTimeCreatedAt() {
        return timeCreatedAt;
    }

    public int getWaitingNumRes() {
        return waitingNumRes;
    }
    
}
