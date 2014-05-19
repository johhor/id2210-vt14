package common.simulation.scenarios;

import com.sun.corba.se.impl.orbutil.closure.Constant;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

@SuppressWarnings("serial")
public class Scenario2 extends Scenario {
    static final int NUM_REQUESTING_PROCESSES = 100;
    static final int NUM_JOINING_PROCESSES = 10;
    static final int NUM_FAILING_PROCESSES = 10;
    
	private static SimulationScenario scenario = new SimulationScenario() {{
            
            for (int i=0; i<NUM_JOINING_PROCESSES; i++){
		SimulationScenario.StochasticProcess process0 = new SimulationScenario.StochasticProcess() {{
			eventInterArrivalTime(constant(1000));
			raise(3, Operations.peerJoin(), 
                                uniform(0, Integer.MAX_VALUE), 
                                constant(8), constant(12000)
                             );
		}};
                process0.start();
                }
            
            for (int i=0; i<NUM_REQUESTING_PROCESSES; i++){
                SimulationScenario.StochasticProcess process1 = new SimulationScenario.StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(100, Operations.requestResources(), 
                                uniform(0, Integer.MAX_VALUE),
                                constant(2), constant(2000),
                                constant(1000*60*1) // 1 minute
                                );
		}};
                process1.start();
            }
            for (int i=0; i<NUM_FAILING_PROCESSES; i++) {
            SimulationScenario.StochasticProcess failPeersProcess = new SimulationScenario.StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(1, Operations.peerFail, 
                                uniform(0, Integer.MAX_VALUE));
            }};
            failPeersProcess.start();
            }
                
                
                SimulationScenario.StochasticProcess process1 = new SimulationScenario.StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(100, Operations.requestResources(), 
                                uniform(0, Integer.MAX_VALUE),
                                constant(2), constant(2000),
                                constant(1000*60*1) // 1 minute
                                );
		}};
                process1.start();
                
		SimulationScenario.StochasticProcess terminateProcess = new SimulationScenario.StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(1, Operations.terminate);
		}};

                terminateProcess.startAfterTerminationOf(100*1000, process1);
	}};

	// -------------------------------------------------------------------
	public Scenario2() {
		super(scenario);
	}
}
