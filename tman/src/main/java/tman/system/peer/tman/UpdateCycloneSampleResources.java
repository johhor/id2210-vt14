/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tman.system.peer.tman;

import common.peer.AvailableResources;
import java.util.ArrayList;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author Johan
 */
public class UpdateCycloneSampleResources{

    public class Request extends Message{
        public Request(Address source,Address destination) {
            super(source, destination);
        }
    }
    
    public class Response extends Message{
        private final AvailableResources availableResources;
        public Response(Address source,Address destination, AvailableResources currentResources) {
            super(source, destination);
            availableResources = currentResources;
        }

        public AvailableResources getAvailableResources() {
            return availableResources;
        }
    }
}
