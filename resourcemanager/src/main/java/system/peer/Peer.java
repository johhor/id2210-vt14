package system.peer;

import java.util.LinkedList;
import java.util.Set;

import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.bootstrap.BootstrapCompleted;
import se.sics.kompics.p2p.bootstrap.BootstrapRequest;
import se.sics.kompics.p2p.bootstrap.BootstrapResponse;
import se.sics.kompics.p2p.bootstrap.P2pBootstrap;
import se.sics.kompics.p2p.bootstrap.PeerEntry;
import se.sics.kompics.p2p.bootstrap.client.BootstrapClient;
import se.sics.kompics.p2p.bootstrap.client.BootstrapClientInit;
import se.sics.kompics.timer.Timer;

import resourcemanager.system.peer.rm.ResourceManager;
import resourcemanager.system.peer.rm.RmInit;
import common.configuration.RmConfiguration;
import common.configuration.CyclonConfiguration;
import common.peer.AvailableResources;
import common.peer.PeerDescriptor;
import cyclon.system.peer.cyclon.*;
import tman.system.peer.tman.TMan;
import tman.system.peer.tman.TManSamplePort;


public final class Peer extends ComponentDefinition {

    	Positive<RmPort> rmPort = positive(RmPort.class);

        Positive<Network> network = positive(Network.class);
	Positive<Timer> timer = positive(Timer.class);
	
        private Component cyclon, tman, rm, bootstrap;
	private Address self;
	private int bootstrapRequestPeerCount;
	private boolean bootstrapped;
	private RmConfiguration rmConfiguration;

        private AvailableResources availableResources;
	
	public Peer() {
		cyclon = create(Cyclon.class);
		tman = create(TMan.class);
		rm = create(ResourceManager.class);
		bootstrap = create(BootstrapClient.class);

		connect(network, rm.getNegative(Network.class));
		connect(network, cyclon.getNegative(Network.class));
		connect(network, bootstrap.getNegative(Network.class));
		connect(network, tman.getNegative(Network.class));
		connect(timer, rm.getNegative(Timer.class));
		connect(timer, cyclon.getNegative(Timer.class));
		connect(timer, bootstrap.getNegative(Timer.class));
		connect(timer, tman.getNegative(Timer.class));
		connect(cyclon.getPositive(CyclonSamplePort.class), 
                        rm.getNegative(CyclonSamplePort.class));
		connect(cyclon.getPositive(CyclonSamplePort.class), 
                        tman.getNegative(CyclonSamplePort.class));
		connect(tman.getPositive(TManSamplePort.class), 
                        rm.getNegative(TManSamplePort.class));

                connect(rmPort, rm.getNegative(RmPort.class));
		
		subscribe(handleInit, control);
		subscribe(handleJoinCompleted, cyclon.getPositive(CyclonPort.class));
		subscribe(handleBootstrapResponse, bootstrap.getPositive(P2pBootstrap.class));
	}

	
	Handler<PeerInit> handleInit = new Handler<PeerInit>() {
                @Override
		public void handle(PeerInit init) {
			self = init.getPeerSelf();
			CyclonConfiguration cyclonConfiguration = init.getCyclonConfiguration();
			rmConfiguration = init.getApplicationConfiguration();
			bootstrapRequestPeerCount = cyclonConfiguration.getBootstrapRequestPeerCount();

                        availableResources = init.getAvailableResources();
                        
			trigger(new CyclonInit(cyclonConfiguration, availableResources), cyclon.getControl());
			trigger(new BootstrapClientInit(self, init.getBootstrapConfiguration()), bootstrap.getControl());
			BootstrapRequest request = new BootstrapRequest("Cyclon", bootstrapRequestPeerCount);
			trigger(request, bootstrap.getPositive(P2pBootstrap.class));
		}
	};


	
	Handler<BootstrapResponse> handleBootstrapResponse = new Handler<BootstrapResponse>() {
                @Override
		public void handle(BootstrapResponse event) {
			if (!bootstrapped) {
				Set<PeerEntry> somePeers = event.getPeers();
				LinkedList<Address> cyclonInsiders = new LinkedList<Address>();
				
				for (PeerEntry peerEntry : somePeers) {
					cyclonInsiders.add(
                                                peerEntry.getOverlayAddress().getPeerAddress());
                                }
				trigger(new CyclonJoin(self, cyclonInsiders), 
                                        cyclon.getPositive(CyclonPort.class));
				bootstrapped = true;
			}
		}
	};

	
	Handler<JoinCompleted> handleJoinCompleted = new Handler<JoinCompleted>() {
                @Override
		public void handle(JoinCompleted event) {
			trigger(new BootstrapCompleted("Cyclon", new PeerDescriptor(self,
                                availableResources.getNumFreeCpus(),
                                availableResources.getFreeMemInMbs())), 
                                bootstrap.getPositive(P2pBootstrap.class));
			trigger(new RmInit(self, rmConfiguration, availableResources), rm.getControl());
		}
	};

}
