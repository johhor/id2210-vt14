package common.simulation.scenarios;

import common.simulation.BatchRequestResource;
import common.simulation.PeerFail;
import common.simulation.PeerJoin;
import common.simulation.RequestResource;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation1;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation3;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation4;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation5;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

@SuppressWarnings("serial")
public class Operations {

    public static Operation3<PeerJoin, Long, Long, Long> peerJoin() {
        return new Operation3<PeerJoin, Long, Long, Long>() {
            @Override
            public PeerJoin generate(Long id, Long numCpus, Long memInMbs) {
                return new PeerJoin(id, numCpus.intValue(), memInMbs.intValue());
            }
        };
    }

    public static Operation1<PeerFail, Long> peerFail = new Operation1<PeerFail, Long>() {
        @Override
        public PeerFail generate(Long id) {
            return new PeerFail(id);
        }
    };

    public static Operation<TerminateExperiment> terminate = new Operation<TerminateExperiment>() {
        @Override
        public TerminateExperiment generate() {
            return new TerminateExperiment();
        }
    };

    public static Operation4<RequestResource, Long, Long, Long, Long> requestResources() {
        return new Operation4<RequestResource, Long, Long, Long, Long>() {
            @Override
            public RequestResource generate(Long id, Long numCpus, Long memInMbs,
                    Long timeToHoldResourceInMilliSecs) {
                return new RequestResource(id, numCpus.intValue(),
                        memInMbs.intValue(),
                        timeToHoldResourceInMilliSecs.intValue());
            }
        };
    }
    
    public static Operation5<BatchRequestResource, Long, Long, Long, Long, Long> BatchRequestResources() {
        return new Operation5<BatchRequestResource, Long, Long, Long, Long, Long>() {
            @Override
            public BatchRequestResource generate(Long id, Long numCpus, Long memInMbs,
            		Long numMachines, Long timeToHoldResourceInMilliSecs) {
                return new BatchRequestResource(id, numCpus.intValue(),
                        memInMbs.intValue(), numMachines.intValue(),
                        timeToHoldResourceInMilliSecs.intValue());
            }
        };
    }
}
