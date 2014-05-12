package tman.system.peer.tman;

import java.util.UUID;

import cyclon.system.peer.cyclon.DescriptorBuffer;
import cyclon.system.peer.cyclon.PeerDescriptor;
import java.util.ArrayList;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

public class ExchangeMsg {

    public static class Request extends Message {

        private static final long serialVersionUID = 8493601671018888143L;
        private final ArrayList<PeerDescriptor> randomBuffer;


        public Request(ArrayList<PeerDescriptor> randomBuffer, Address source, 
                Address destination) {
            super(source, destination);
            this.randomBuffer = randomBuffer;
        }

        public ArrayList<PeerDescriptor> getRandomBuffer() {
            return randomBuffer;
        }


        public int getSize() {
            return 0;
        }
    }

    public static class Response extends Message {

        private static final long serialVersionUID = -5022051054665787770L;
        private final ArrayList<PeerDescriptor> selectedBuffer;
        //private final DescriptorBuffer selectedBuffer;

        public Response(ArrayList<PeerDescriptor> selectedBuffer, Address source, Address destination) {
            super(source, destination);
            this.selectedBuffer = selectedBuffer;
        }

//        public Response(UUID requestId, DescriptorBuffer selectedBuffer, Address source, Address destination) {
//            super(source, destination);
//            this.requestId = requestId;
//            this.selectedBuffer = selectedBuffer;
//        }


        public ArrayList<PeerDescriptor> getSelectedBuffer() {
            return selectedBuffer;
        }

        public int getSize() {
            return 0;
        }
    }

    public static class RequestTimeout extends Timeout {

        private final Address peer;


        public RequestTimeout(ScheduleTimeout request, Address peer) {
            super(request);
            this.peer = peer;
        }


        public Address getPeer() {
            return peer;
        }
    }
}