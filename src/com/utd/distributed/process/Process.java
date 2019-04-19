package com.utd.distributed.process;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.utd.distributed.master.Master;
import com.utd.distributed.util.Message;

public class Process implements Runnable{

	private Master master;
	private int root;
    private int id;
    private ArrayList<Integer> neighbors;
    private Process[] p;
    private int parent;
    private volatile String messageFromMaster = "";
    private int round = 0;
	private boolean processDone = false;
	private boolean msgSent = false;
	private HashMap<Integer,String> status = new HashMap<Integer,String>();
	
	private volatile Queue<Message> messageQueue = new LinkedList<>();
	private volatile boolean marked = false;
    
	public Process(int id, int root,int[] edgeList,Master master) {
		this.id = id;
		this.root = root;
		int count = 0;
		this.neighbors = new ArrayList<Integer>();
		this.master = master;
		for(Integer i : edgeList){
			if(i == 1)
				neighbors.add(count); 
			count++;
		}
		
		//To reset all the acknowledgement status for all its neighbors
		acknowledgeStatus(id,"",true);
	}
    
	public void setProcessNeighbors(Process p[]) {
		this.p = p;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		master.roundCompletionForProcess(id);
		//System.out.println("Process "+id+" ready");
		while(!processDone){
			
			if(getMessageFromMaster().equals("Initiate")){
				//System.out.println("Process "+id+" entered initiate");
				setMessageFromMaster("");
				if(id == root){
					//System.out.println("Root is "+ id);
					sendMessages();
					this.msgSent = true;
					this.marked = true;
					this.parent = id ;
				}
				master.roundCompletionForProcess(id);	
			}else if (getMessageFromMaster().equals("StartRound")){
				
				round++;
				//System.out.println("Process "+id+" entered start round"+round);
				setMessageFromMaster("");
				
				//System.out.println("If process:" + id + "is marked :"+marked);
				if(!this.msgSent && marked) {
					sendMessages();
					msgSent=true;
				}
				receiveMessages();
				master.roundCompletionForProcess(id);
			}else if(getMessageFromMaster().equals("SendParent")){
								
				setMessageFromMaster("");
				master.assignParents(id,parent);
				master.roundCompletionForProcess(id);
			}else if(getMessageFromMaster().equals("MasterDone")){
				// Terminating the Algorithm
				processDone = true;
			}
		}
	}
	
	public String getMessageFromMaster(){
		return messageFromMaster;
	}
	
	
	public void setMessageFromMaster(String messageFromMaster) {
		this.messageFromMaster = messageFromMaster;
	}
	
	private void sendMessages(){
		Message message = new Message();
		message.setFromId(this.id);
		message.setSentRound(round);
		for (int n : neighbors) {
			if(n != id) {
				p[n].modifyQueue(message, "insert");
			}
		}
	}
		
	// Processing messages in the Queue 
	private boolean receiveMessages(){
		
		Message msg = modifyQueue(null, "poll");
		while(msg != null) {
			if(this.marked == false) {
				//System.out.println("message not null for ID" + id);
				this.parent = msg.getFromId();
				//System.out.println("Parent of "+ id + " is :" + parent);
				p[id].acknowledgeStatus(id, "Known", false);
				this.marked = true;
			}else {
				p[msg.getFromId()].acknowledgeStatus(id, "Reject", false);
			}
			msg = new Message();
			msg = modifyQueue(null,"poll");
		}
		if (acknowledge(id)) {
			//System.out.print("Acknowledged process is "+id+" parent is "+parent);
			if(parent == id){
				Master.treeDone = true;
				//System.out.println("Tree Done");
			}
			else {
				//System.out.println("Acknoledge done for Id"+id);
				p[parent].acknowledgeStatus(id, "Done", false);
			}
		}
		return marked;
	}

		
		/* Check if all the child processes are done and all the acknowledgement
		 * messages are collected back.
		 * * */
		public synchronized boolean acknowledge(int id){
			for (Map.Entry<Integer, String> m : status.entrySet()) {
				if (m.getKey() != parent && m.getValue().equals("Unknown")) {
					return false;
				}
			}
			return true;
		}
		
		
		/*
		 * Checks whether a process still need to send explore message or not
		 * */
		public synchronized boolean acknowledgeStatus(int id, String reply, boolean reset){
			if(reset){
				for(Integer val : neighbors){
						status.put(val,"Unknown");
				}
			}else{
				status.put(id, reply);
			}
			return true;
		}
		
		/*
		 * Insert, Poll or reset the message Queue
		 * */
		public synchronized Message modifyQueue(Message msg, String action){
			
			if("insert".equalsIgnoreCase(action)) {
				messageQueue.add(msg);
			}
			else if("poll".equalsIgnoreCase(action)) {
				if (!messageQueue.isEmpty())
					return messageQueue.poll();
			}
			else if("reset".equalsIgnoreCase(action)) {
				while(!messageQueue.isEmpty()) ;
			}
			return null;
		}
}
