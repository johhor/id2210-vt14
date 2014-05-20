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
public class BestSearchResponse {
    private final int numCpus;
    private final int amountMemInMb;
    private final int time;
    private final boolean isCpuMsg;
    private final double timeCreatedAt;
    SearchResourceMsg.Response bestResponse;
    
    public BestSearchResponse(int numCpus, int amountMemInMb, int time, boolean isCpuMsg, SearchResourceMsg.Response bestResponse, double createdAt) {
        this.numCpus = numCpus;
        this.amountMemInMb = amountMemInMb;
        this.time =time;
        this.isCpuMsg = isCpuMsg;
        this.bestResponse = bestResponse;
        this.timeCreatedAt = createdAt;
    }
    
    public void replaceBestResponse(SearchResourceMsg.Response response) {
        bestResponse = response;
    }

    public SearchResourceMsg.Response getBestResponse() {
        return bestResponse;
    }

    public boolean isCpuMsg() {
        return isCpuMsg;
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
    public double getTimeCreatedAt() {
        return timeCreatedAt;
    }
}
