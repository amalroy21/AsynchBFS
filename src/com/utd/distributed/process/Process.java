package com.utd.distributed.process;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import com.utd.distributed.master.Master;
import com.utd.distributed.util.Message;

public class Process implements Runnable{
	private int id;
	private int parent;
	private long distance;
	private Master master;
	private Process p[];
	private HashMap<Integer,Integer> neighborsAndDistance;
	private HashMap<Integer,String> status = new HashMap<Integer,String>();
	private volatile HashMap<Integer, BlockingQueue<Message>> link = new HashMap<>();
	private int round = 0;
	private boolean processDone = false;
	private volatile String messageFromMaster = "";
	private Random random = new Random();
	private int transmissiontime=0;
	private int intialRound = round;
	public Process(int id, HashMap<Integer,Integer> neighborsAndDistance, Master master, int parent, long distance){
		this.id = id;
		this.parent = parent;
		this.distance = distance;
		this.master = master;
		this.neighborsAndDistance = new HashMap<>();
		this.neighborsAndDistance = neighborsAndDistance;
		for(Integer a : neighborsAndDistance.keySet()){
			status.put(a, "Unknown");
			BlockingQueue<Message> queue = new LinkedBlockingDeque<>();
			link.put(a, queue);
		}
	}
	
	public void setProcessNeighbors(Process p[]) {
		this.p = p;
	}
	
	@Override
	public void run() {
		
		master.roundCompletionForProcess(id);
		
		while(!processDone){
			if(getMessageFromMaster().equals("Initiate")){
				setMessageFromMaster("");
				if(distance == 0){
					transmissiontime = 1+random.nextInt(15);
					intialRound = round;
				}
				master.roundCompletionForProcess(id);	
			}else if (getMessageFromMaster().equals("StartRound")){
				
				round++;
				setMessageFromMaster("");
				boolean change = receiveMessages();
				
				if(change){
					transmissiontime = 1+random.nextInt(15);
					intialRound = round;
					//System.out.println("Process "+id+" Change happened at round "+round +" transmission delay:"+transmissiontime);
				}
				if(transmissiontime!=0 && round > transmissiontime + intialRound) {
					
					sendMessages();
					transmissiontime = 0;
				}else if(distance !=0) {
					sendDummyMessage();
				}
				master.roundCompletionForProcess(id);
			}else if(getMessageFromMaster().equals("SendParent")){
				setMessageFromMaster("");
				int weight;
				if(id == parent)
					weight = 0;
				else{
					weight = neighborsAndDistance.get(parent);
				}
				master.assignParents(id,parent,weight);
				master.roundCompletionForProcess(id);
			}else if(getMessageFromMaster().equals("MasterDone")){
				// Terminating the Algorithm
				System.out.println("Process: " + id + "; Parent: " + parent + "; Distance from Source: " + distance);
				processDone = true;
			}
		}
	}
	
	// Processing messages in the Queue and updating the current distance
	private boolean receiveMessages(){
		boolean flag = false;
		for (Integer neighbor : link.keySet()) {
			Message msg = modifyQueue(null, false, neighbor, false);
			while(msg != null ){
				if(!msg.isDummy()) {
					//System.out.println("process:"+id+"at round:"+round+"Message Received :"+msg.toString());
					long newDistance = msg.getCurDistance() + neighborsAndDistance.get(msg.getFromId());
					//System.out.println("Process "+id+" New distance is "+newDistance+" and old distance "+distance);
					if(newDistance < distance){	// Relaxation
						flag = true;
						this.distance = newDistance;
						if(parent != -1){
							p[parent].acknowledgeStatus(id,"Reject",false);
						}
						this.parent = msg.getFromId();
						p[id].acknowledgeStatus(id, "Unknown", true);
					}else{
						p[msg.getFromId()].acknowledgeStatus(id, "Reject", false);
						//System.out.println("Process "+id+" Rejected process:"+msg.getFromId());
						//System.out.println("Process :"+id+" Acknowledgement check "+acknowledge(id));
					}
				}
				
				msg = modifyQueue(null, false, neighbor, false);
			}
			if (acknowledge(id)) {
				System.out.print("Acknowledged process is "+id+" parent is "+parent);
				if(parent == id){
					Master.treeDone = true;
				}
				else {
					p[parent].acknowledgeStatus(id, "Done", false);
				}
			}
		}
		
		return flag;
	}
	
	/*
	 * If for a particular process if it's unable to relax other process weights then it sends I'm done to parent.
	 * If source recieves I'm done from all it's neighbours it terminates algorithm by updating treeDone of master
	 * */
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
	public synchronized boolean acknowledgeStatus(int nid, String reply, boolean reset){
		if(reset){
			for(Integer val : neighborsAndDistance.keySet()){
				status.put(val,"Unknown");
			}
			return true;
		}else{
			status.put(nid, reply);
		}
		return true;
	}
	
	// Sending current distance to all neighbors
	private void sendMessages(){
		Message message = new Message();
		message.setFromId(this.id);
		message.setCurDistance(this.distance);
		for (int n : neighborsAndDistance.keySet()) {
			//System.out.println("Send message from process "+id+" to process:"+n+" With transmission delay"+transmissiontime+" round"+round);
			message.setTransmissionTime(transmissiontime);
			message.setSentRound(round);
			message.setDummy(false);
			p[n].modifyQueue(message, true, this.id, false);
			master.setMessageCounter();
		}
	}
	
	// Sending Dummy Message to all neighbors
	private void sendDummyMessage(){
		Message message = new Message();
		message.setFromId(this.id);
		message.setDummy(true);
		for (int n : neighborsAndDistance.keySet()) {
			message.setSentRound(round);
			//p[n].modifyQueue(message, true, this.id, false);
			master.setMessageCounter();
		}
	}
	
	
	/*
	 * To modify the input queue of the process
	 * 
	 * @insert = true implies to insert message into the Queue (Message sent
	 * from neighbor)
	 * 
	 * @insert = false implies to poll the first message in the Queue (Message
	 * received by the Process)
	 */
	public synchronized Message modifyQueue(Message msg, boolean insert, int fromid, boolean check){
		if (check) {
			for (Queue<Message> queue : link.values()) {
				if (!queue.isEmpty())
					return queue.peek();
			}
			return null;
		} else {
			BlockingQueue<Message> queue;
			if (insert) {
				queue = new LinkedBlockingDeque<>();
				queue.add(msg);
				link.put(fromid, queue);
				return msg;
			} else {
				queue = link.get(fromid);
				if (!queue.isEmpty()) {
					Message m = queue.remove();
					return m;
				} else
					return null;
			}
		}
	}
	
	/* To get the recent message send by the master process. 
	 * in order to execute the distributed systems synchronously
	 * */
	public String getMessageFromMaster(){
		return messageFromMaster;
	}
	
	/*
	 * To set the signal by the master process to processes. 
	 * in order to execute the distributed systems synchronously
	 * */
	public void setMessageFromMaster(String messageFromMaster) {
		this.messageFromMaster = messageFromMaster;
	}
	
}