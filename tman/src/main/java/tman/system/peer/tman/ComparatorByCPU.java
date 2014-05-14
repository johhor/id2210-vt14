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
public class ComparatorByCPU implements Comparator<PeerDescriptor> {
    PeerDescriptor self;

    public ComparatorByCPU(PeerDescriptor self) {
        this.self = self;
    }

    @Override
    public int compare(PeerDescriptor o1, PeerDescriptor o2) {
        assert (o1.getAvailableResources().getNumFreeCpus() == o2.getAvailableResources().getNumFreeCpus());
        if (o1.getAvailableResources().getNumFreeCpus() < self.getAvailableResources().getNumFreeCpus() && o2.getAvailableResources().getNumFreeCpus() > self.getAvailableResources().getNumFreeCpus()) {
            return 1;
        } else if (o2.getAvailableResources().getNumFreeCpus() < self.getAvailableResources().getNumFreeCpus() && o1.getAvailableResources().getNumFreeCpus() > self.getAvailableResources().getNumFreeCpus()) {
            return -1;
        } else if (Math.abs(o1.getAvailableResources().getNumFreeCpus() - self.getAvailableResources().getNumFreeCpus()) < Math.abs(o2.getAvailableResources().getNumFreeCpus() - self.getAvailableResources().getNumFreeCpus())) {
            return -1;
        }
        return 1;
    }
    
}
