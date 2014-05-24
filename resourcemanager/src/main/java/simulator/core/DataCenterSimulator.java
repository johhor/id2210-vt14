package simulator.core;

import common.simulation.SimulatorPort;
import java.util.HashMap;

import se.sics.kompics.ChannelFilter;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.bootstrap.BootstrapConfiguration;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;

import system.peer.Peer;
import system.peer.PeerInit;
import simulator.snapshot.Snapshot;
import common.configuration.RmConfiguration;
import common.configuration.Configuration;
import common.configuration.CyclonConfiguration;
import common.configuration.TManConfiguration;
import common.peer.AvailableResources;
import common.simulation.BatchRequestResource;
import common.simulation.ConsistentHashtable;
import common.simulation.GenerateReport;
import common.simulation.PeerFail;
import common.simulation.PeerJoin;
import common.simulation.RequestResource;
import common.simulation.SimulatorInit;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Random;
import se.sics.ipasdistances.AsIpGenerator;
import system.peer.RmPort;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

public final class DataCenterSimulator extends ComponentDefinition {

    Positive<SimulatorPort> simulator = positive(SimulatorPort.class);
    Positive<Network> network = positive(Network.class);
    Positive<Timer> timer = positive(Timer.class);
    private final HashMap<Long, Component> peers;
    private final HashMap<Long, Address> peersAddress;
    private BootstrapConfiguration bootstrapConfiguration;
    private CyclonConfiguration cyclonConfiguration;
    private RmConfiguration rmConfiguration;
    private TManConfiguration tmanConfiguration;
    private Long identifierSpaceSize;
    private ConsistentHashtable<Long> ringNodes;
    private AsIpGenerator ipGenerator = AsIpGenerator.getInstance(125);
    
    Random r = new Random(System.currentTimeMillis());
	
    public DataCenterSimulator() {
        peers = new HashMap<Long, Component>();
        peersAddress = new HashMap<Long, Address>();
        ringNodes = new ConsistentHashtable<Long>();

        subscribe(handleInit, control);
        subscribe(handleGenerateReport, timer);
        subscribe(handlePeerJoin, simulator);
        subscribe(handlePeerFail, simulator);
        subscribe(handleTerminateExperiment, simulator);
        subscribe(handleRequestResource, simulator);
        subscribe(handleBatchRequestResource, simulator);
    }
	
    Handler<SimulatorInit> handleInit = new Handler<SimulatorInit>() {
        @Override
        public void handle(SimulatorInit init) {
            peers.clear();

            bootstrapConfiguration = init.getBootstrapConfiguration();
            cyclonConfiguration = init.getCyclonConfiguration();
            rmConfiguration = init.getAggregationConfiguration();
            tmanConfiguration = init.getTmanConfiguration();
            
            identifierSpaceSize = cyclonConfiguration.getIdentifierSpaceSize();

            // generate periodic report
            int snapshotPeriod = Configuration.SNAPSHOT_PERIOD;
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(snapshotPeriod, snapshotPeriod);
            spt.setTimeoutEvent(new GenerateReport(spt));
            trigger(spt, timer);

        }
    };
        
    Handler<RequestResource> handleRequestResource = new Handler<RequestResource>() {
        @Override
        public void handle(RequestResource event) {
            Long successor = ringNodes.getNode(event.getId());
            Component peer = peers.get(successor);
            trigger( event, peer.getNegative(RmPort.class));
        }
    };
    
    Handler<BatchRequestResource> handleBatchRequestResource = new Handler<BatchRequestResource>() {
        @Override
        public void handle(BatchRequestResource event) {
            Long successor = ringNodes.getNode(event.getId());
            Component peer = peers.get(successor);
            trigger( event, peer.getNegative(RmPort.class));
        }
    };
	
    Handler<PeerJoin> handlePeerJoin = new Handler<PeerJoin>() {
        @Override
        public void handle(PeerJoin event) {
            Long id = event.getPeerId();

            // join with the next id if this id is taken
            Long successor = ringNodes.getNode(id);

            while (successor != null && successor.equals(id)) {
                id = (id +1) % identifierSpaceSize;
                successor = ringNodes.getNode(id);
            }

            createAndStartNewPeer(id, event.getNumFreeCpus(), 
                    event.getFreeMemoryInMbs());
            ringNodes.addNode(id);
        }
    };
	
    Handler<PeerFail> handlePeerFail = new Handler<PeerFail>() {
        @Override
        public void handle(PeerFail event) {
            Long id = ringNodes.getNode(event.getId());

            if (ringNodes.size() == 0) {
                System.err.println("Empty network");
                return;
            }

            ringNodes.removeNode(id);
            stopAndDestroyPeer(id);
        }
    };
	
    Handler<TerminateExperiment> handleTerminateExperiment = new Handler<TerminateExperiment>() {
        @Override
        public void handle(TerminateExperiment event) {
        	//Fix statistics
        	String[] stats = null;
        	try{
        		  FileInputStream fstream = new FileInputStream("temp.tst");
        		  DataInputStream in = new DataInputStream(fstream);
        		  BufferedReader br = new BufferedReader(new InputStreamReader(in));
        		  String strLine;

        		  if ((strLine = br.readLine()) != null)   {
        			  stats = strLine.split(",");
        		  }
        		  in.close();
        		  File file = new File("temp.tst");
        		  file.delete();
        	}catch (Exception e){//Catch exception if any
        		  System.err.println("Error: " + e.getMessage());
        	}
        	try {
        		File file = new File("Statistics.tst");
      		    file.delete();
                PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("Statistics"+".tst",true)));
                int i = 0;
                for (String s : stats) {
                	i++;
                    if (i>=100) {
                        i=0;
                        writer.println("");
                    }
                    writer.print(s+",");
                }
                writer.flush();
                writer.close();
            } catch (IOException ex) {
                
            }

            System.err.println("Finishing experiment - terminating....");
            System.exit(0);
        }
    };
    
    Handler<GenerateReport> handleGenerateReport = new Handler<GenerateReport>() {
        @Override
        public void handle(GenerateReport event) {
            Snapshot.report();
        }
    };

	
    private void createAndStartNewPeer(long id, int numCpus, int memInMb) {
        Component peer = create(Peer.class);
        InetAddress ip = ipGenerator.generateIP();
        Address address = new Address(ip, 8058, (int) id);

        connect(network, peer.getNegative(Network.class), new MessageDestinationFilter(address));
        connect(timer, peer.getNegative(Timer.class));
        
        AvailableResources ar = new AvailableResources(numCpus, memInMb);
        trigger(new PeerInit(address, bootstrapConfiguration, cyclonConfiguration, 
                rmConfiguration, ar), peer.getControl());

        trigger(new Start(), peer.getControl());
        peers.put(id, peer);
        peersAddress.put(id, address);
        Snapshot.addPeer(address, ar);
    }

	
    private void stopAndDestroyPeer(Long id) {
        Component peer = peers.get(id);

        trigger(new Stop(), peer.getControl());

        disconnect(network, peer.getNegative(Network.class));
        disconnect(timer, peer.getNegative(Timer.class));

        peers.remove(id);
        Address addr = peersAddress.remove(id);
        Snapshot.removePeer(addr);

        destroy(peer);
    }

	
    private final static class MessageDestinationFilter extends ChannelFilter<Message, Address> {

        public MessageDestinationFilter(Address address) {
            super(Message.class, address, true);
        }

        @Override
        public Address getValue(Message event) {
            return event.getDestination();
        }
    }
}
