package tman.system.peer.tman;

import common.peer.AvailableResources;
import cyclon.system.peer.cyclon.PeerDescriptor;
import se.sics.kompics.address.Address;
/**
 *
 * @author Johan
 */
public class GradientPeerDescriptor extends PeerDescriptor{
    AvailableResources availableResources;

    public GradientPeerDescriptor(Address rMAddress, AvailableResources rMResources) {
        super(rMAddress);
        availableResources = rMResources;
    }

    public AvailableResources getAvailableResources() {
        return availableResources;
    }

    public void setAvailableResources(AvailableResources availableResources) {
        this.availableResources = availableResources;
    }
    
    
}
