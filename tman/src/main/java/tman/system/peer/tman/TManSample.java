package tman.system.peer.tman;

import cyclon.system.peer.cyclon.PeerDescriptor;
import java.util.ArrayList;


import se.sics.kompics.Event;
import se.sics.kompics.address.Address;


public class TManSample extends Event {
	ArrayList<PeerDescriptor> partners = new ArrayList<PeerDescriptor>();
        boolean isCPU;

	public TManSample(ArrayList<PeerDescriptor> partners, boolean isCPUMsg) {
		this.partners = partners;
                isCPU = isCPUMsg;
	}
        
	public TManSample() {
	}

    public boolean isCPU() {
        return isCPU;
    }


	public ArrayList<PeerDescriptor> getSample() {
		return this.partners;
	}
}
