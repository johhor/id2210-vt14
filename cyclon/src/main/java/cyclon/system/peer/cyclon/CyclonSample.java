package cyclon.system.peer.cyclon;

import java.util.ArrayList;
import se.sics.kompics.Event;
import se.sics.kompics.address.Address;

public class CyclonSample extends Event {
	ArrayList<GradientPeerDescriptor> nodes = new ArrayList<GradientPeerDescriptor>();


	public CyclonSample(ArrayList<GradientPeerDescriptor> nodes) {
		this.nodes = nodes;
	}
        
	public CyclonSample() {
	}

	public ArrayList<GradientPeerDescriptor> getSample() {
		return this.nodes;
	}
}
