package com.utd.distributed.master;

import java.util.HashMap;
import com.utd.distributed.process.Process;

public class Master implements Runnable{

   private int ProcessCount;
   private Process[] p;
   private volatile HashMap<Integer,Integer> parents = new HashMap<>();
   private volatile HashMap<Integer,Boolean> roundDetails = new HashMap<>();
   private boolean masterDone = false;
   public static boolean treeDone = false;
   public int root;
   
   public Master(int ProcessCount, int root){
		this.ProcessCount = ProcessCount;
		for(int i = 0; i < ProcessCount; i++){
			roundDetails.put(i, false);
		}
		this.root = root;
	}
   
   // Passing the Reference of the Process
   public void setProcesses(Process[] p){
		this.p = p;
		for(int i = 0; i < ProcessCount; i++){
			p[i].setProcessNeighbors(p);
		}
   }
   
   @Override
   public void run() {

	   System.out.println("Master Process has started");
		
		while (!roundDone()) {
			// Waiting till all the Processes have started
		}
		resetRoundDetails();
		initiateProcesses();
		while (!roundDone()) {
			//Initiating the messages from all the Processes
		}
		resetRoundDetails();
		startRound();
		while(!masterDone){
			while (!roundDone()) {
				// Waiting till all the Processes complete one round
			}
			while(!treeDone){
				resetRoundDetails();
				startRound();
				while(!roundDone()){
					// Waiting till all the Processes complete one round
				}
			}
			resetRoundDetails();
			getParents();
			while(!roundDone()){
				// Waiting for all Processes to send parents
			}
			printTree();
			stopMasterProcess();
			masterDone = true;
		}
	}
   
   // Assigning parents for each Process
	public synchronized void assignParents(int id, int parent) {
		// TODO Auto-generated method stub
		parents.put(id, parent);
	}
	
	// Sending Terminate message to all the Processes
	public void stopMasterProcess(){
		for(int i = 0; i < ProcessCount; i++){
			p[i].setMessageFromMaster("MasterDone");
		}
	}
	
	// Collecting round completion information from each Process
	public synchronized void roundCompletionForProcess(int id){
		roundDetails.put(id, true);
	}
	
	// Constructing and printing the Minimum Spanning Tree
	public void printTree(){
		int result[][] = new int[ProcessCount][ProcessCount];
		for(int i = 0; i < result.length; i++){
			for(int j = 0; j < result[0].length;j++){
				result[i][j] = -1;
			}
		}
		
		for(Integer id : parents.keySet()){
			if(id == parents.get(id)){
				result[id][parents.get(id)] = -1;
				result[parents.get(id)][id] = -1;
			}else{
			result[id][parents.get(id)] = parents.get(id);
			result[parents.get(id)][id] = parents.get(id);
			}
		}
		System.out.println("Synch BFS Algorithm is Executed !!");
		System.out.println("Following is the resultant Minimum Spanning Tree as a Adjacency List: ");
		System.out.println("Process"+"\t"+"Neighbours:");
		for (int i = 0; i < ProcessCount; i++) {
			System.out.print(i+"\t");
			for (int j = 0; j < ProcessCount; j++) {
				if(result[i][j] != - 1){
				System.out.print(j+"\t");
				}
			}
			System.out.println();
		}
		
	}
	
	
	// Request for parent Process for each Process for building the Shortest
	// path tree
	private void getParents() {
		for (int i = 0; i < ProcessCount; i++) {
			p[i].setMessageFromMaster("SendParent");
		}
	}
	
	// To start next round
	private void startRound() {
		for (int i = 0; i < ProcessCount; i++) {
			p[i].setMessageFromMaster("StartRound");
		}
	}
	
	// To start next round
	private void initiateProcesses() {
		for (int i = 0; i < ProcessCount; i++) {
			p[i].setMessageFromMaster("Initiate");
		}
	}
	
	// Reset the Round confirmation after each round
	private void resetRoundDetails(){
		for(int i = 0; i < ProcessCount; i++){
			roundDetails.put(i, false);
		}
	}
	
	// To check the completion of the Round
	private boolean roundDone(){
		for(boolean b : roundDetails.values()){
			if(!b){
				return false;
			}
		}
		return true;
	}
}
