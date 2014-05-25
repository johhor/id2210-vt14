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
public class ComparatorByMEM extends QueueLengthComparer implements Comparator<PeerDescriptor> {

    public ComparatorByMEM(PeerDescriptor self) {
        super(self);
    }

    @Override
    public int compare(PeerDescriptor o1, PeerDescriptor o2) {
        if (o1.getAvailableResources().getFreeMemInMbs()== o2.getAvailableResources().getFreeMemInMbs() && o1.getAvailableResources().getQueueLength() == o2.getAvailableResources().getQueueLength())
            return 0;
        if (o1.getAvailableResources().getQueueLength()>0 || o2.getAvailableResources().getQueueLength()>0) {
            return super.queueCompare(o1, o2);
        } 
        else if (o1.getAvailableResources().getNumFreeCpus() == o2.getAvailableResources().getNumFreeCpus()) {
        	return 0;
        } else if (o1.getAvailableResources().getFreeMemInMbs() <= self.getAvailableResources().getFreeMemInMbs() && o2.getAvailableResources().getFreeMemInMbs() > self.getAvailableResources().getFreeMemInMbs()) {
            return 1;
        } else if (o2.getAvailableResources().getFreeMemInMbs() <= self.getAvailableResources().getFreeMemInMbs() && o1.getAvailableResources().getFreeMemInMbs() > self.getAvailableResources().getFreeMemInMbs()) {
            return -1;
        } 
        else if (Math.abs(o1.getAvailableResources().getFreeMemInMbs() - self.getAvailableResources().getFreeMemInMbs()) == Math.abs(o2.getAvailableResources().getFreeMemInMbs() - self.getAvailableResources().getFreeMemInMbs())) {
        	return 0;
        }
        else if (Math.abs(o1.getAvailableResources().getFreeMemInMbs() - self.getAvailableResources().getFreeMemInMbs()) < Math.abs(o2.getAvailableResources().getFreeMemInMbs() - self.getAvailableResources().getFreeMemInMbs())) {
            return -1;
        }
        return 1;
    }
    
}
