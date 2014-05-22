package main;

import simulator.core.DataCenterSimulationMain;
import common.configuration.Configuration;
import common.simulation.scenarios.BatchScenario;
import common.simulation.scenarios.Scenario;
import common.simulation.scenarios.Scenario1;
import common.simulation.scenarios.Scenario2;

public class Main {

    public static void main(String[] args) throws Throwable {
        // TODO - change the random seed, have the user pass it in.
        long seed = System.currentTimeMillis();
        Configuration configuration = new Configuration(seed);

        Scenario scenario = new BatchScenario();
        scenario.setSeed(seed);
        scenario.getScenario().simulate(DataCenterSimulationMain.class);
    }
}
