/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package resourcemanager.system.peer.rm;

import org.mortbay.jetty.handler.ResourceHandler;

/**
 *
 * @author Linus
 */
public class RequestEntity {
    private final int numCpus;
    private final int amountMemInMb;
    private final int time;
    private final long timeCreatedAt;
    int waitingNumRes;
    RequestResources.Response bestResponse;
    
    public RequestEntity(int waitingNumRes, int numCpus, int amountMemInMb, int time, long createdAt) {
        this.waitingNumRes = waitingNumRes;
        this.numCpus = numCpus;
        this.amountMemInMb = amountMemInMb;
        this.time =time;
        timeCreatedAt = createdAt;
    }
    

	public RequestResources.Response isBestResponse(RequestResources.Response response) {
        waitingNumRes--;
        if (bestResponse == null) {
            bestResponse = response;
        } else if (!bestResponse.getSuccess() && response.getSuccess()) {
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
    public long getTimeCreatedAt() {
		return timeCreatedAt;
	}
}