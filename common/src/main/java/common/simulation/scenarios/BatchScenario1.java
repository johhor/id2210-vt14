package common.simulation.scenarios;

import com.sun.corba.se.impl.orbutil.closure.Constant;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

@SuppressWarnings("serial")
public class BatchScenario1 extends Scenario {
	static final int NUM_PROCESSES = 200;

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
					eventInterArrivalTime(constant(2000));
					raise(200, Operations.BatchRequestResources(),
							uniform(0, Integer.MAX_VALUE), constant(4),
							constant(1), constant(5), constant(60000) // 6sec
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
			terminateProcess.startAfterTerminationOf(100000, process1);
		}
	};

	// -------------------------------------------------------------------
	public BatchScenario1() {
		super(scenario);
	}
}
