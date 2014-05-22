package system.peer;

import common.simulation.BatchRequestResource;
import common.simulation.RequestResource;
import se.sics.kompics.PortType;

public class RmPort extends PortType {{
	positive(RequestResource.class);
	positive(BatchRequestResource.class);
}}
