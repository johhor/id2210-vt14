package simulator.core;

import common.configuration.Configuration;
import common.configuration.CyclonConfiguration;
import common.configuration.RmConfiguration;
import common.configuration.TManConfiguration;
import common.peer.AvailableResources;
import common.peer.RunTimeStatistics;
import common.simulation.BatchRequestResource;
import common.simulation.ConsistentHashtable;
import common.simulation.GenerateReport;
import common.simulation.PeerFail;
import common.simulation.PeerJoin;
import common.simulation.RequestResource;
import common.simulation.SimulatorInit;
import common.simulation.SimulatorPort;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.sics.ipasdistances.AsIpGenerator;
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
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;
import simulator.snapshot.Snapshot;
import system.peer.Peer;
import system.peer.PeerInit;
import system.peer.RmPort;

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
    private RunTimeStatistics stat;
    
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
        stat = new RunTimeStatistics();
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
            System.err.println("Finishing experiment - terminating....");
//            printStatistics();
            System.exit(0);
        }
    };
    
    private void printStatistics(){
        try{
            int i = 1;
            String line;
            BufferedReader br = new BufferedReader(new FileReader("testStat.tst"));
            while ((line = br.readLine()) != null) {    
                String[] timeStrings = line.split(","); 
                for(String time : timeStrings){
                    System.out.println((i++)+". "+"Time is: "+ time);
                    stat.addAllocationTime(Long.parseLong(time));
                    if(i > 5000)
                        break;
                }
               if(i > 5000)
                        break;
            }
            br.close();
            stat.printAllData("testStatFinal.tst");
            System.out.println("99:th percentile of AllocationTime: "+stat.get99thPercentileAllocationTimes());
            System.out.println("Mean value of AllocationTime: "+stat.getAllocationTimeMeanValue());
            }catch(IOException ex){
                System.err.println("Problems reading statistics");
            }
    }
    
    Handler<GenerateReport> handleGenerateReport = new Handler<GenerateReport>() {
        @Override
        public void handle(GenerateReport event) {
            //Snapshot.report();
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
                rmConfiguration, tmanConfiguration, ar), peer.getControl());

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
