package common.simulation;

import se.sics.kompics.Event;

public final class BatchRequestResource extends Event {
    
    private final long id;
    private final int numCpus;
    private final int memoryInMbs;
    private final int timeToHoldResource;
    private final int numMachines;

    public BatchRequestResource(long id, int numCpus, int memoryInMbs, int numMachines, int timeToHoldResource) {
        this.id = id;
        this.numCpus = numCpus;
        this.memoryInMbs = memoryInMbs;
        this.numMachines = numMachines;
        this.timeToHoldResource = timeToHoldResource;
    }

    public long getId() {
        return id;
    }

    public int getTimeToHoldResource() {
        return timeToHoldResource;
    }

    public int getMemoryInMbs() {
        return memoryInMbs;
    }

    public int getNumCpus() {
        return numCpus;
    }
    public int getNumMachines() {
    	return numMachines;
    }
}

