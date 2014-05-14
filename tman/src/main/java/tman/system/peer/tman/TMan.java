package tman.system.peer.tman;

import common.configuration.TManConfiguration;
import common.peer.AvailableResources;
import common.peer.UpdateAvailableResources;
import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import cyclon.system.peer.cyclon.PeerDescriptor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import tman.simulator.snapshot.Snapshot;

public final class TMan extends ComponentDefinition {

    static final int SAMPLE_SIZE = 10;

    private static final Logger logger = LoggerFactory.getLogger(TMan.class);

    Negative<TManSamplePort> tmanPort = negative(TManSamplePort.class);
    Negative<TManUpdateAvailableResourcesPort> tmanUARPort = negative(TManUpdateAvailableResourcesPort.class);
    Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);

    private long period;
    private Address self;
    private ArrayList<PeerDescriptor> tmanCPUPartners;
    private ArrayList<PeerDescriptor> tmanMEMPartners;
    private ArrayList<PeerDescriptor> randomView = new ArrayList<PeerDescriptor>();
    private TManConfiguration tmanConfiguration;
    private Random r;
    private AvailableResources availableResources;

    private int randomViewUpdateCounter;
    private int currId;

    public class TManSchedule extends Timeout {

        public TManSchedule(SchedulePeriodicTimeout request) {
            super(request);
        }

        public TManSchedule(ScheduleTimeout request) {
            super(request);
        }
    }

    public TMan() {
        tmanCPUPartners = new ArrayList<PeerDescriptor>();
        tmanMEMPartners = new ArrayList<PeerDescriptor>();
        subscribe(handleInit, control);
        subscribe(handleRound, timerPort);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleTManPartnersResponse, networkPort);
        subscribe(handleTManPartnersRequest, networkPort);
        subscribe(handleUpdateAvailableResources, tmanUARPort);
    }

    Handler<TManInit> handleInit = new Handler<TManInit>() {
        @Override
        public void handle(TManInit init) {
            self = init.getSelf();
            tmanConfiguration = init.getConfiguration();
            period = tmanConfiguration.getPeriod();
            r = new Random(tmanConfiguration.getSeed());
            availableResources = init.getAvailableResources();
            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period, period);
            rst.setTimeoutEvent(new TManSchedule(rst));
            trigger(rst, timerPort);
            currId = 0;
        }
    };
    /*Creates a new Gradient descriptor with age 0*/

    private PeerDescriptor getSelf() {
        return new PeerDescriptor(self, availableResources);
    }

    /*When we get a time out from our timer containing ar TManSchedule is equals
     to doing this each delta time unit. Each delta we choose a random peer in 
     our view and requests to exchange our view with it.
     */
    Handler<TManSchedule> handleRound = new Handler<TManSchedule>() {
        @Override
        public void handle(TManSchedule event) {
            handleTmanSchedule(tmanCPUPartners,true);
            handleTmanSchedule(tmanMEMPartners,false);
        }
    };
    
    private void handleTmanSchedule(ArrayList<PeerDescriptor> tmanPartners, boolean isCPU){
        ArrayList<Address> tmanPartnersForSnapshot = new ArrayList<Address>();
            for (PeerDescriptor pd : tmanPartners) {
                tmanPartnersForSnapshot.add(pd.getAddress());
                //increment age
                pd.incrementAndGetAge();
            }
            
            Snapshot.updateTManPartners(self, tmanPartnersForSnapshot);

            if (!tmanPartners.isEmpty()) {
                Address p = selectPeer(isCPU).getAddress();

                ArrayList<PeerDescriptor> buff = new ArrayList<PeerDescriptor>(tmanPartners);

                merge(buff, randomView);
                PeerDescriptor selfGPD = getSelf();
                if (!buff.contains(selfGPD)) {
                    buff.add(selfGPD);
                }
                trigger(new ExchangeMsg.Request(buff, self, p, isCPU), networkPort);
                
                //TODO Timeout
            }
    }
    
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            randomViewUpdateCounter = 0;
            randomView = event.getSample();
            if (tmanCPUPartners.isEmpty()) {
                merge(tmanCPUPartners,randomView);
            }
            if (tmanMEMPartners.isEmpty()) {
                merge(tmanMEMPartners,randomView);
            }

        }
    };
    /**  
     Handler<UpdateCycloneSampleResources.Request> handleUpdateCycloneSampleResourcesRequest = new Handler<UpdateCycloneSampleResources.Request>(){
     @Override
     public void handle(UpdateCycloneSampleResources.Request e) {
     }
     };

            
     Handler<UpdateCycloneSampleResources.Response> handleUpdateCycloneSampleResourcesResponce = new Handler<UpdateCycloneSampleResources.Response>() {
 
     @Override
     public void handle(UpdateCycloneSampleResources.Response e) {
     GradientPeerDescriptor sampleGPD = new GradientPeerDescriptor(e.getSource(), e.getAvailableResources());
     randomView.remove(sampleGPD);
     randomView.add(sampleGPD);
     }
     };
     **/

    Handler<ExchangeMsg.Request> handleTManPartnersRequest = new Handler<ExchangeMsg.Request>() {
        @Override
        public void handle(ExchangeMsg.Request event) {
            ArrayList<PeerDescriptor> tmanPartners = event.isCPU() ? tmanCPUPartners : tmanMEMPartners;
            ArrayList<PeerDescriptor> buff = new ArrayList<PeerDescriptor>(tmanPartners);
            merge(buff, randomView);
            PeerDescriptor ownPD = getSelf();
            buff.remove(ownPD);
            buff.add(ownPD);
            trigger(new ExchangeMsg.Response(buff, self, event.getSource(), event.isCPU), networkPort);
            merge(tmanPartners, event.getRandomBuffer());
            selectView(tmanPartners, event.isCPU());
        }
    };

    Handler<ExchangeMsg.Response> handleTManPartnersResponse = new Handler<ExchangeMsg.Response>() {
        @Override
        public void handle(ExchangeMsg.Response event) {
            ArrayList<PeerDescriptor> tmanPartners = event.isCPU() ? tmanCPUPartners : tmanMEMPartners;
            merge(tmanPartners, event.getSelectedBuffer());
            selectView(tmanPartners, event.isCPU());
            // Publish sample to connected components
            trigger(new TManSample(tmanPartners,event.isCPU), tmanPort);
        }
    };

    Handler<UpdateAvailableResources> handleUpdateAvailableResources = new Handler<UpdateAvailableResources>() {
        @Override
        public void handle(UpdateAvailableResources e) {
            availableResources = e.getAvailableResources();
        }
    };

    /*Selects a random peer from the the top 50% of peers in the current view*/
    private PeerDescriptor selectPeer(boolean isCPU) {
        ArrayList<PeerDescriptor> buffer = isCPU ? 
                new ArrayList<PeerDescriptor>(tmanCPUPartners) : new ArrayList<PeerDescriptor>(tmanMEMPartners);
        ArrayList<PeerDescriptor> sample = new ArrayList<PeerDescriptor>();
        int loopLength = buffer.size();
        for (int i = 0; i < loopLength; i++) {
            sample.add(isCPU ? getSoftMaxCpu(buffer) : getSoftMaxMem(buffer));  
            buffer.remove(sample.get(i));
        }
        return sample.get(r.nextInt((int) Math.ceil(sample.size() / 2.0)));
    }
    /* Selects a view from a buffer, taking 1/2 of the addresses using the 
     getSoftMax function */

    private void selectView(ArrayList<PeerDescriptor> buf, boolean isCPU) {
        ArrayList<PeerDescriptor> temp = new ArrayList<PeerDescriptor>(buf);
        buf.clear();
        int newViewSize = temp.size() / 2;
        for (int i = 0; i < newViewSize; i++) {      
            buf.add(isCPU ? getSoftMaxCpu(temp) : getSoftMaxMem(temp) );
            temp.remove(buf.get(i));
        }
    }
    /*Merges two lists of addresses into the first argument*/
    private void merge(List<PeerDescriptor> buf, List<PeerDescriptor> view) {
        for (PeerDescriptor a : view) {
            if(buf.contains(a)){
                if(a.getAge() < buf.get(buf.indexOf(a)).getAge()){
                    buf.remove(a);//Older descriptor
                    buf.add(a); //Add the newer descriptor instead
                }
            }
            else if (!buf.contains(a.clone())) {
                buf.add(a);
            }
        }
    }
    
    /**
     * TODO - if you call this method with a list of entries, it will return a
     * single node, weighted towards the 'best' node (as defined by
     * ComparatorById) with the temperature controlling the weighting. A
     * temperature of '1.0' will be greedy and always return the best node. A
     * temperature of '0.000001' will return a random node. A temperature of
     * '0.0' will throw a divide by zero exception :) Reference:
     http://webdocs.cs.ualberta.ca/~sutton/book/2/node4.html*
     */
    
    public PeerDescriptor getSoftMaxCpu(List<PeerDescriptor> entries) {
        Collections.sort(entries, new ComparatorByCPU(getSelf()));

        double rnd = r.nextDouble();
        double total = 0.0d;
        double[] values = new double[entries.size()];
        int j = entries.size() + 1;
        /*Initializes the valueslist*/
        for (int i = 0; i < entries.size(); i++) {
            double val = j--;
            values[i] = Math.exp(val / tmanConfiguration.getTemperature());
            total += values[i];
        }

        for (int i = 0; i < values.length; i++) {
            if (i != 0) {
                values[i] += values[i - 1];
            }
            // normalise the probability for this entry
            double normalisedUtility = values[i] / total;
            if (normalisedUtility >= rnd) {
                return entries.get(i);
            }
        }
        return entries.get(entries.size() - 1);
    }

    public PeerDescriptor getSoftMaxMem(List<PeerDescriptor> entries) {
        Collections.sort(entries, new ComparatorByMEM(getSelf()));

        double rnd = r.nextDouble();
        double total = 0.0d;
        double[] values = new double[entries.size()];
        int j = entries.size() + 1;
        /*Initializes the valueslist*/
        for (int i = 0; i < entries.size(); i++) {
            double val = j--;
            values[i] = Math.exp(val / tmanConfiguration.getTemperature());
            total += values[i];
        }

        for (int i = 0; i < values.length; i++) {
            if (i != 0) {
                values[i] += values[i - 1];
            }
            // normalise the probability for this entry
            double normalisedUtility = values[i] / total;
            if (normalisedUtility >= rnd) {
                return entries.get(i);
            }
        }
        return entries.get(entries.size() - 1);
    }
}
