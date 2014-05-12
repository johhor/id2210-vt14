package tman.system.peer.tman;

import common.configuration.TManConfiguration;
import common.peer.AvailableResources;
import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
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
    Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    private long period;
    private Address self;
    private ArrayList<GradientPeerDescriptor> tmanPartners;
    private ArrayList<GradientPeerDescriptor> randomView = new ArrayList<GradientPeerDescriptor>();
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
        tmanPartners = new ArrayList<GradientPeerDescriptor>();

        subscribe(handleInit, control);
        subscribe(handleRound, timerPort);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleTManPartnersResponse, networkPort);
        subscribe(handleTManPartnersRequest, networkPort);
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
    private GradientPeerDescriptor getSelf(){
        return new GradientPeerDescriptor(self, availableResources);
    }
    
    /*When we get a time out from our timer containing ar TManSchedule is equals
    to doing this each delta time unit. Each delta we choose a random peer in 
    our view and requests to exchange our view with it.
    */
    Handler<TManSchedule> handleRound = new Handler<TManSchedule>() {
        @Override
        public void handle(TManSchedule event) {
            ArrayList<Address> tmanPartnersForSnapshot = new ArrayList<Address>();
            for (GradientPeerDescriptor gpd : tmanPartners)
                tmanPartnersForSnapshot.add(gpd.getAddress());
            
            Snapshot.updateTManPartners(self, tmanPartnersForSnapshot);
            
            if (!tmanPartners.isEmpty()) {
            Address p = selectPeer().getAddress();
            
            ArrayList<GradientPeerDescriptor> buff = new ArrayList<GradientPeerDescriptor>(tmanPartners);
            
            merge(buff, randomView);
            GradientPeerDescriptor selfGPD = getSelf();
            if(!buff.contains(selfGPD))
                buff.add(selfGPD);
            
            trigger(new ExchangeMsg.Request(buff,self,p),networkPort);
            }
//            ScheduleTimeout st = new ScheduleTimeout(period);
//            st.setTimeoutEvent(new ExchangeMsg.RequestTimeout(st, p));
//            trigger()
        }
    };
    /*Selects a random peer from the the top 50% of peers in the current view*/
    private GradientPeerDescriptor selectPeer(){
        ArrayList<GradientPeerDescriptor> buffer = new ArrayList<GradientPeerDescriptor>(tmanPartners);
        ArrayList<GradientPeerDescriptor> sample = new ArrayList<GradientPeerDescriptor>();
        for (int i = 0; i < tmanPartners.size(); i++) {
                sample.add(getSoftMaxAddress(buffer));
                buffer.remove(sample.get(i));
        }
        return sample.get(r.nextInt((int)Math.ceil(sample.size()/2.0)));
    }
    /* Selects a view from a buffer, taking 1/2 of the addresses using the 
    getSoftMax function */
    private void selectView(ArrayList<GradientPeerDescriptor> buf){
        ArrayList<GradientPeerDescriptor> temp = new ArrayList<GradientPeerDescriptor>(buf);

        buf.clear();
        int newViewSize = temp.size()/2;
        for (int i = 0; i < newViewSize ; i++) {
            buf.add(getSoftMaxAddress(temp));
            temp.remove(buf.get(i));
        }
    }
    /*Merges two lists of addresses into the first argument*/
    private void merge(List<Address> buf, List<Address> view){
            for(Address a : view){
                if(!buf.contains(a))
                   buf.add(a);
            }
    }
    
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            randomViewUpdateCounter = 0;
            if (tmanPartners.isEmpty()) {
                tmanPartners = event.getSample();
            }
            randomView = event.getSample();
        }
    };
  /*  
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
*/
            
    Handler<ExchangeMsg.Request> handleTManPartnersRequest = new Handler<ExchangeMsg.Request>() {
        @Override
        public void handle(ExchangeMsg.Request event) {
            ArrayList<GradientPeerDescriptor> buff = new ArrayList<GradientPeerDescriptor>(tmanPartners);
            merge(buff, randomView);
            GradientPeerDescriptor ownGPD = getSelf();
            buff.remove(ownGPD);
            buff.add(ownGPD);
            trigger(new ExchangeMsg.Response(buff, self, event.getSource()), networkPort);
            merge(tmanPartners, event.getRandomBuffer());
            selectView(tmanPartners);
        }
    };

    Handler<ExchangeMsg.Response> handleTManPartnersResponse = new Handler<ExchangeMsg.Response>() {
        @Override
        public void handle(ExchangeMsg.Response event) {
            merge(tmanPartners,event.getSelectedBuffer());
            selectView(tmanPartners);
            // Publish sample to connected components
            trigger(new TManSample(tmanPartners), tmanPort);
        }
    };

    /** TODO - if you call this method with a list of entries, it will
     return a single node, weighted towards the 'best' node (as defined by
     ComparatorById) with the temperature controlling the weighting.
     A temperature of '1.0' will be greedy and always return the best node.
     A temperature of '0.000001' will return a random node.
     A temperature of '0.0' will throw a divide by zero exception :)
     Reference:
     http://webdocs.cs.ualberta.ca/~sutton/book/2/node4.html**/
    public GradientPeerDescriptor getSoftMaxAddress(List<GradientPeerDescriptor> entries) {
        //Collections.sort(entries, new ComparatorById(self));

        double rnd = r.nextDouble();
        double total = 0.0d;
        double[] values = new double[entries.size()];
        //int j = entries.size() + 1;
        /*Initializes the valueslist*/
        for (int i = 0; i < entries.size(); i++) {
            // get inverse of values - lowest have highest value.
//            double val = j;
            double val = 0.0000000000000000000000000000000000000000000;
//            j--;
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
