/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package resourcemanager.system.peer.rm;

import common.peer.AvailableResources;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

public class SearchResourceMsg {
    
    public static class Request extends Message{
        
        private boolean cpuMsg;
        private int msgId;
        
        public Request(Address source, Address destination, boolean isCPU,int id) {
            super(source, destination);
            cpuMsg = isCPU;
            msgId = id;
        }

        public boolean isCpuMsg() {
            return cpuMsg;
        }

        public int getMsgId() {
            return msgId;
        }
        
    }
    
    public static class Response extends Message{
        private Address nextNode;
        private AvailableResources askedNodesResources;
        private int msgId;
        
        public Response(Address source, Address destination, Address bestNode, AvailableResources myResouces, int id) {
            super(source, destination);
            nextNode = bestNode;
            askedNodesResources = myResouces;
            msgId = id;
        }
        public Address getNextNode() {
            return nextNode;
        }

        public AvailableResources getAskedNodesResources() {
            return askedNodesResources;
        }      

        public int getMsgId() {
            return msgId;
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

    
}
