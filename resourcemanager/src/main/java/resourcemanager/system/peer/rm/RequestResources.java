package resourcemanager.system.peer.rm;

import java.util.List;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

/**
 * User: jdowling
 */
public class RequestResources  {

    public static class Request extends Message {

        private final int numCpus;
        private final int amountMemInMb;
        private final int id;
        
        public Request(Address source, Address destination, int numCpus, int amountMemInMb, int id) {
            super(source, destination);
            this.numCpus = numCpus;
            this.amountMemInMb = amountMemInMb;
            this.id = id;
        }

        public int getAmountMemInMb() {
            return amountMemInMb;
        }

        public int getNumCpus() {
            return numCpus;
        }
        public int getId(){
            return id;
        }

    }
    
    public static class Response extends Message {
        private final int queueSize;
        private final boolean success;
        private  final  int id;
        public Response(Address source, Address destination, boolean success, int queueSize, int id) {
            super(source, destination);
            this.queueSize = queueSize;
            this.success = success;
            this.id = id;
        }
        
        public int getQueueSize(){
            return queueSize;
        }
        
        public boolean getSuccess(){
            return success;
        }
        public int getId(){
            return id;
        }
        
    }
    
    public static class RequestTimeout extends Timeout {
        private final Address destination;
        RequestTimeout(ScheduleTimeout st, Address destination) {
            super(st);
            this.destination = destination;
        }

        public Address getDestination() {
            return destination;
        }
    }
}
