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
    private RequestHandler requestHandler;
    SearchResourceMsg.Response bestResponse;
    
    public BestSearchResponse(RequestHandler rh, SearchResourceMsg.Response bestResponse) {
        this.requestHandler = rh;
        this.bestResponse = bestResponse;
    }
    
    public void replaceBestResponse(SearchResourceMsg.Response response) {
        bestResponse = response;
    }

    public SearchResourceMsg.Response getBestResponse() {
        return bestResponse;
    }

    public RequestHandler getRequestHandler() {
    	return requestHandler;
    }
}
