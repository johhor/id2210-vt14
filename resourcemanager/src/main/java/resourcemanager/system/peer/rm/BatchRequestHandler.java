package resourcemanager.system.peer.rm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;


import common.simulation.BatchRequestResource;
import cyclon.system.peer.cyclon.PeerDescriptor;

import se.sics.kompics.address.Address;

public class BatchRequestHandler extends RequestHandler{
	
	private ArrayList<Address> availableNodes;
	private ArrayList<RequestResources.Response> busyNodes;
	
	int numMachines;
	
	public BatchRequestHandler(int numRespToWaitOn, BatchRequestResource brr, long timeStartedAt){
		super(numRespToWaitOn,brr.getNumCpus(),brr.getMemoryInMbs(),brr.getTimeToHoldResource(),timeStartedAt);
		availableNodes = new ArrayList<Address>();
		busyNodes = new ArrayList<RequestResources.Response>();
		numMachines = brr.getNumMachines();
	}
	@Override
	public boolean isBatch(){
		return true;
	}
	
	public boolean allMachinesCanBeAllocated(){
		return availableNodes.size()+busyNodes.size() >= waitingNumRes;
	}
	public boolean isAllocatable() {
		return availableNodes.size()+ busyNodes.size()>= numMachines;
	}
	public boolean allResponsesReceived(){
		return availableNodes.size()>= numMachines;
	}
	
	public void addResponce(RequestResources.Response e){
		boolean notInSelected = !availableNodes.contains(e.getSource());
		//Only distinct allocatable  nodes without a queue are put in selected nodes
		if(e.getQueueSize() == 0 && e.getSuccess() && notInSelected){
			this.availableNodes.add(e.getSource());
		}
		else if (!busyNodes.contains(e) && notInSelected){
			//Else we store them untill we have to use them with data to get best suited
			busyNodes.add(e);
		}
	}
	
	public 	ArrayList<Address> getNodes(){
		ArrayList<Address> nodes = new ArrayList<Address>(numMachines);
		Comparator<RequestResources.Response> comp = getComp();
		Collections.sort(busyNodes, comp);
		
		//pick nodes until number of machines is reached
		while (nodes.size()<numMachines) {
			for (int i = 0; i < availableNodes.size() && nodes.size()<numMachines; i++) {
				nodes.add(availableNodes.get(i));
			}
		
			for (int i = 0; i < busyNodes.size() && nodes.size()<numMachines; i++) {
				nodes.add(busyNodes.get(i).getSource());
			}
		}
		return nodes;
	}
	
	private Comparator<RequestResources.Response> getComp(){
		return new Comparator<RequestResources.Response>() {
			@Override
			public int compare(RequestResources.Response o1, RequestResources.Response o2) {
				return Integer.compare(o1.getQueueSize(), o2.getQueueSize());
			}
		};
	}
	public ArrayList<Address> getSelectedNodes() {
		return availableNodes;
	}
	public void setSelectedNodes(ArrayList<Address> selectedNodes) {
		this.availableNodes = selectedNodes;
	}
}