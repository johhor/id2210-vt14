
package tman.system.peer.tman;

import cyclon.system.peer.cyclon.PeerDescriptor;

public class QueueLengthComparer {
    PeerDescriptor self;
    
    public QueueLengthComparer(PeerDescriptor self){
        this.self = self;
    }
       public int queueCompare(PeerDescriptor o1, PeerDescriptor o2) {
        if (o1.getAvailableResources().getQueueLength() == o2.getAvailableResources().getQueueLength())
            return 0;
        if (o1.getAvailableResources().getQueueLength() < self.getAvailableResources().getQueueLength() && o2.getAvailableResources().getQueueLength() > self.getAvailableResources().getQueueLength()) {
            return 1;
        } else if (o2.getAvailableResources().getQueueLength() < self.getAvailableResources().getQueueLength() && o1.getAvailableResources().getQueueLength() > self.getAvailableResources().getQueueLength()) {
            return -1;
        } else if (Math.abs(o1.getAvailableResources().getQueueLength() - self.getAvailableResources().getQueueLength()) < Math.abs(o2.getAvailableResources().getQueueLength() - self.getAvailableResources().getQueueLength())) {
            return -1;
        }
        return 1;
    }
}