package com.utd.distributed.master;

import java.util.HashMap;

import com.utd.distributed.process.Parent;
import com.utd.distributed.process.Process;

public class Master implements Runnable{
	
	
	
	
	private int processCount;
	private volatile HashMap<Integer,Boolean> roundDetails = new HashMap<>();
	private volatile HashMap<Integer,Parent> parents = new HashMap<>();
	private Process[] p;
	private boolean masterDone = false;
	public static volatile boolean treeDone = false;
	private int messageCounter=0;
	
	public Master(int processCount){
		this.processCount = processCount;
		for(int i = 0; i < processCount; i++){
			roundDetails.put(i, false);
		}
	}
	
	public void setProcesses(Process[] p){
		this.p = p;
		for(int i = 0; i < processCount; i++){
			p[i].setProcessNeighbors(p);
		}
	}

	@Override
	public void run() {

		System.out.println("Master Process started");
		
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
	public synchronized void assignParents(int id, int parent, int weight) {
		Parent p = new Parent(parent,weight);
		parents.put(id, p);
	}
	
	// Sending Terminate message to all the Processes
	public void stopMasterProcess(){
		for(int i = 0; i < processCount; i++){
			p[i].setMessageFromMaster("MasterDone");
		}
	}
	
	// Collecting round completion information from each Process
	public synchronized void roundCompletionForProcess(int id){
		roundDetails.put(id, true);
	}
	
	// Constructing and printing the Shortest Paths Tree
	public void printTree(){
		System.out.println("Asynchronous BFS Algorithm is Executed Successfully!!");
		int result[][] = new int[processCount][processCount];
		for(int i = 0; i < result.length; i++){
			for(int j = 0; j < result[0].length;j++){
				result[i][j] = -1;
			}
		}
		for(Integer id : parents.keySet()){
			if(id == parents.get(id).parent){
				result[id][parents.get(id).parent] = -1;
				result[parents.get(id).parent][id] = -1;
			}else{
			result[id][parents.get(id).parent] = parents.get(id).weight;
			result[parents.get(id).parent][id] = parents.get(id).weight;
			}
		}
		
		System.out.println("Following is the resultant BFS Tree as an Adjacency List: ");
		System.out.println("Process"+"\t"+"Neighbours");
		for (int i = 0; i < processCount; i++) {
			System.out.print(i+"\t");
			for (int j = 0; j < processCount; j++) {
				if(result[i][j] != - 1){
				System.out.print(j+"\t");//+" : "+result[i][j] + "\t");
				}
			}
			System.out.println();
		}
		System.out.println("Total No of Messages :"+getMessageCounter());
		
	}
	
	private void getParents() {
		for (int i = 0; i < processCount; i++) {
			p[i].setMessageFromMaster("SendParent");
		}
	}
	
	// To start next round
	private void startRound() {
		for (int i = 0; i < processCount; i++) {
			p[i].setMessageFromMaster("StartRound");
		}
	}
	
	// To initiate the processes
	private void initiateProcesses() {
		for (int i = 0; i < processCount; i++) {
			p[i].setMessageFromMaster("Initiate");
		}
	}
	
	// Reset the Round confirmation after each round
	private void resetRoundDetails(){
		for(int i = 0; i < processCount; i++){
			roundDetails.put(i, false);
		}
	}
	
	// To check the completion of the Round
	private boolean roundDone(){
		//System.out.println("round done entered");
		for(boolean b : roundDetails.values()){
			if(!b){
				return false;
			}
		}
		
		return true;
	}
	
	public int getMessageCounter() {
		return messageCounter;
	}

	public void setMessageCounter() {
		this.messageCounter++;
	}
	
	
	
}
