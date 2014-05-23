package common.simulation.scenarios;

import com.sun.corba.se.impl.orbutil.closure.Constant;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

@SuppressWarnings("serial")
public class BatchScenario2 extends Scenario {
	static final int NUM_PROCESSES = 20;

	private static SimulationScenario scenario = new SimulationScenario() {
		{

			SimulationScenario.StochasticProcess process0 = new SimulationScenario.StochasticProcess() {
				{
					eventInterArrivalTime(constant(1000));
					raise(NUM_PROCESSES, Operations.peerJoin(),
							uniform(0, Integer.MAX_VALUE), constant(8),
							constant(12000));
				}
			};

			SimulationScenario.StochasticProcess process1 = null;

			process1 = new SimulationScenario.StochasticProcess() {
				{
					eventInterArrivalTime(constant(35));
					raise(80, Operations.BatchRequestResources(),
							uniform(0, Integer.MAX_VALUE), constant(2),
							constant(2000), constant(5), constant(100 * 60 * 1) // 1// minute
					);
				}
			};
                        SimulationScenario.StochasticProcess process2 = null;
                        process2 = new SimulationScenario.StochasticProcess() {
                            {
                                eventInterArrivalTime(constant(1));
                                raise(180, Operations.BatchRequestResources(),
                                        uniform(0, Integer.MAX_VALUE), constant(2),
                                        constant(2000), constant(5), constant(1 * 60 * 1) // 1 minute
					);
				}
			};

			SimulationScenario.StochasticProcess failPeersProcess = new SimulationScenario.StochasticProcess() {
				{
					eventInterArrivalTime(constant(100));
					raise(1, Operations.peerFail, uniform(0, Integer.MAX_VALUE));
				}
			};
			// failPeersProcess.start();

			SimulationScenario.StochasticProcess terminateProcess = new SimulationScenario.StochasticProcess() {
				{
					eventInterArrivalTime(constant(100));
					raise(1, Operations.terminate);
				}
			};
			
			process0.start();
			process1.startAfterTerminationOf(2000, process0);
                        process2.startAfterTerminationOf(0, process1);
			terminateProcess.startAfterTerminationOf(20000, process2);
		}
	};

	// -------------------------------------------------------------------
	public BatchScenario2() {
		super(scenario);
	}
}
