package tman.system.peer.tman;

import java.util.ArrayList;


import se.sics.kompics.Event;
import se.sics.kompics.address.Address;


public class TManSample extends Event {
	ArrayList<GradientPeerDescriptor> partners = new ArrayList<GradientPeerDescriptor>();


	public TManSample(ArrayList<GradientPeerDescriptor> partners) {
		this.partners = partners;
	}
        
	public TManSample() {
	}


	public ArrayList<GradientPeerDescriptor> getSample() {
		return this.partners;
	}
}
