package resourcemanager.system.peer.rm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;


import common.simulation.BatchRequestResource;
import cyclon.system.peer.cyclon.PeerDescriptor;

import se.sics.kompics.address.Address;

public class BatchRequestHandler extends RequestHandler{
	
	private ArrayList<Address> selectedNodes;
	private ArrayList<RequestResources.Response> busyList;
	
	int numMachines;
	
	public BatchRequestHandler(int numRespToWaitOn, BatchRequestResource brr, long timeStartedAt){
		super(numRespToWaitOn,brr.getNumCpus(),brr.getMemoryInMbs(),brr.getTimeToHoldResource(),timeStartedAt);
		selectedNodes = new ArrayList<Address>();
		busyList = new ArrayList<RequestResources.Response>();
		numMachines = brr.getNumMachines();
	}
	@Override
	public boolean isBatch(){
		return true;
	}
	
	public boolean allMachinesCanBeAllocated(){
		return selectedNodes.size()+busyList.size() >= waitingNumRes;
	}
	public boolean isAllocatable() {
		return selectedNodes.size()+ busyList.size()>= numMachines;
	}
	public boolean allResponsesReceived(){
		return selectedNodes.size()>= numMachines;
	}
	
	public void addResponce(RequestResources.Response e){
		//Only distinct allocatable  nodes without a queue are put in selected nodes
		if(e.getQueueSize() == 0 && e.getSuccess() && !selectedNodes.contains(e.getSource())){
			this.selectedNodes.add(e.getSource());
		}
		else{
			//Else we store them untill we have to use them with data to get best suited
			busyList.add(e);
		}
	}
	public 	ArrayList<Address> getNodes(){
		ArrayList<Address> nodes = new ArrayList<Address>(numMachines);
		for(Address a : selectedNodes) {
			nodes.add(a);
		}
		
		int resOfAddresses = numMachines - selectedNodes.size();
		ArrayList<Address> rest = getRestFromBadList();
		
		for (int i = 0; i < resOfAddresses; i++) {
			nodes.add(rest.remove(0));
		}
		return nodes;
	}
	
	private	ArrayList<Address> getRestFromBadList(){
		ArrayList<Address> distinctNodes = getDistinctRestFromBadList();
		if(distinctNodes.size()+selectedNodes.size() >= numMachines)//Can we satisfy the need with distinct nodes?
			return distinctNodes;
		
		return getRestFromBadList(busyList,false);
	}
	private ArrayList<Address> getDistinctRestFromBadList(){
		ArrayList<Address> tmp = new ArrayList<Address>();
		ArrayList<RequestResources.Response> distinctList = new ArrayList<RequestResources.Response>();
		for (RequestResources.Response r : busyList) {
			if(!tmp.contains(r.getSource())){
				tmp.add(r.getSource());
				distinctList.add(r);
			}
		}
		return getRestFromBadList(distinctList,true);
	}

	private ArrayList<Address> getRestFromBadList(ArrayList<RequestResources.Response> list, boolean distinct){
		int toIndex = numMachines - selectedNodes.size();
		ArrayList<Address> rest = new ArrayList<Address>();
		Address a;
		for (int i = 0; i < toIndex; i++) {
			
			if(!distinct)
				a = removeBestFromList(list);
			else
				a = getBestFromList(list);
			updateBusyList(list,a);
			rest.add(a);
		}
		return rest;
	}
	private Address removeBestFromList(ArrayList<RequestResources.Response> rest){
		Comparator<RequestResources.Response> comp = getComp();
		Collections.sort(rest, comp);
		return rest.remove(0).getSource();
	}
	private Address getBestFromList(ArrayList<RequestResources.Response> rest){
		Comparator<RequestResources.Response> comp = getComp();
		Collections.sort(rest, comp);
		return rest.get(0).getSource();
	}
	private void updateBusyList(ArrayList<RequestResources.Response> list, Address a){
		for (int i = 0; i < list.size(); i++) {
			if(list.get(i).getSource().equals(a))
				list.get(i).incrementQueueSize();
		}
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
		return selectedNodes;
	}
	public void setSelectedNodes(ArrayList<Address> selectedNodes) {
		this.selectedNodes = selectedNodes;
	}
}