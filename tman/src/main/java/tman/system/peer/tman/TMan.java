package tman.system.peer.tman;

import common.configuration.TManConfiguration;
import common.peer.AvailableResources;
import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import cyclon.system.peer.cyclon.DescriptorBuffer;
import cyclon.system.peer.cyclon.PeerDescriptor;
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
    private ArrayList<Address> tmanPartners;
    private ArrayList<Address> randomView;
    private TManConfiguration tmanConfiguration;
    private Random r;
    private AvailableResources availableResources;
    
    int currId;
    
    public class TManSchedule extends Timeout {

        public TManSchedule(SchedulePeriodicTimeout request) {
            super(request);
        }

        public TManSchedule(ScheduleTimeout request) {
            super(request);
        }
    }

    public TMan() {
        tmanPartners = new ArrayList<Address>();

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

    Handler<TManSchedule> handleRound = new Handler<TManSchedule>() {
        @Override
        public void handle(TManSchedule event) {
            Snapshot.updateTManPartners(self, tmanPartners);
            
            Address p = selectPeer();
            
            ArrayList<Address> buff = new ArrayList<Address>(tmanPartners);
            
            merge(buff, randomView);
            
            if(!buff.contains(self))
                buff.add(self);
            
            trigger(new ExchangeMsg.Request(buff,self,p),networkPort);
//            ScheduleTimeout st = new ScheduleTimeout(period);
//            st.setTimeoutEvent(new ExchangeMsg.RequestTimeout(st, p));
//            trigger()
        }
    };
    
    private Address selectPeer(){
        ArrayList<Address> buffer = new ArrayList<Address>(tmanPartners);
        ArrayList<Address> sample = new ArrayList<Address>();
        for (int i = 0; i < tmanPartners.size(); i++) {
                sample.add(getSoftMaxAddress(buffer));
                buffer.remove(sample.get(i));
        }
        return sample.get(r.nextInt(sample.size()/2));
    }
    
    private void selectView(ArrayList<Address> buf){
        ArrayList<Address> temp = new ArrayList<Address>(buf);

        buf.clear();
        int newViewSize = temp.size()/2;
        for (int i = 0; i < newViewSize ; i++) {
            buf.add(getSoftMaxAddress(temp));
            temp.remove(buf.get(i));
        }
    }
    
    private void merge(List<Address> buf, List<Address> view){
            for(Address a : view){
                if(!buf.contains(a))
                   buf.add(a);
            }
    }

    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            List<Address> cyclonPartners = event.getSample();
            merge(tmanPartners,cyclonPartners);
            if(!tmanPartners.contains(self))
                tmanPartners.add(self);

        }
    };

    Handler<ExchangeMsg.Request> handleTManPartnersRequest = new Handler<ExchangeMsg.Request>() {
        @Override
        public void handle(ExchangeMsg.Request event) {
            ArrayList<Address> buff = new ArrayList<Address>(tmanPartners);
            merge(buff, randomView);
            if(!buff.contains(self))
                buff.add(self);
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

    // TODO - if you call this method with a list of entries, it will
    // return a single node, weighted towards the 'best' node (as defined by
    // ComparatorById) with the temperature controlling the weighting.
    // A temperature of '1.0' will be greedy and always return the best node.
    // A temperature of '0.000001' will return a random node.
    // A temperature of '0.0' will throw a divide by zero exception :)
    // Reference:
    // http://webdocs.cs.ualberta.ca/~sutton/book/2/node4.html
    public Address getSoftMaxAddress(List<Address> entries) {
        Collections.sort(entries, new ComparatorById(self));

        double rnd = r.nextDouble();
        double total = 0.0d;
        double[] values = new double[entries.size()];
        int j = entries.size() + 1;
        for (int i = 0; i < entries.size(); i++) {
            // get inverse of values - lowest have highest value.
            double val = j;
            j--;
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
