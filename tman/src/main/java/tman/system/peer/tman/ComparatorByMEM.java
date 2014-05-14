/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tman.system.peer.tman;

import cyclon.system.peer.cyclon.PeerDescriptor;
import java.util.Comparator;
import se.sics.kompics.address.Address;

/**
 * Make Node with Highest Id Leader in the Gradient
 */
public class ComparatorByMEM implements Comparator<PeerDescriptor> {
    PeerDescriptor self;

    public ComparatorByMEM(PeerDescriptor self) {
        this.self = self;
    }

    @Override
    public int compare(PeerDescriptor o1, PeerDescriptor o2) {
        assert (o1.getAvailableResources().getFreeMemInMbs()== o2.getAvailableResources().getFreeMemInMbs());
        if (o1.getAvailableResources().getFreeMemInMbs() < self.getAvailableResources().getFreeMemInMbs() && o2.getAvailableResources().getFreeMemInMbs() > self.getAvailableResources().getFreeMemInMbs()) {
            return 1;
        } else if (o2.getAvailableResources().getFreeMemInMbs() < self.getAvailableResources().getFreeMemInMbs() && o1.getAvailableResources().getFreeMemInMbs() > self.getAvailableResources().getFreeMemInMbs()) {
            return -1;
        } else if (Math.abs(o1.getAvailableResources().getFreeMemInMbs() - self.getAvailableResources().getFreeMemInMbs()) < Math.abs(o2.getAvailableResources().getFreeMemInMbs() - self.getAvailableResources().getFreeMemInMbs())) {
            return -1;
        }
        return 1;
    }
    
}
