package resourcemanager.system.peer.rm;

import java.util.List;

import javassist.expr.Instanceof;
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
        private int queueSize;
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
        @Override
        public boolean equals(Object o) {
        	if (o instanceof Response) {
        		//used for distinct source in batchRequestHandler
        		return ((Response) o).getSource().getId() == this.getSource().getId();
        	} else {
        		return false;
        	}
        }
    }
    
    public static class RequestTimeout extends Timeout {
        private final int id;
        RequestTimeout(ScheduleTimeout st, int id) {
            super(st);
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }
    
    public static class Allocate extends Message {
        private final int numCpus;
        private final int amountMemInMb;
        private final int time;
        private final long timeCreatedAt;

        public Allocate(Address src, Address dest ,int numCpus, int amountMemInMb, int time, long createdAt) {
            super(src, dest);
            this.numCpus = numCpus;
            this.amountMemInMb = amountMemInMb;
            this.time = time;
            timeCreatedAt = createdAt;
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
}
