package com.utd.distributed.process;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

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
	//private volatile Queue<Message> messageQueue = new LinkedList<Message>();
	private volatile HashMap<Integer, Queue<Message>> link = new HashMap<>();
	private int round = 0;
	private boolean processDone = false;
	private volatile String messageFromMaster = "";
	private Random random = new Random();
	//public Process(){}
	public Process(int id, HashMap<Integer,Integer> neighborsAndDistance, Master master, int parent, long distance){
		this.id = id;
		this.parent = parent;
		this.distance = distance;
		this.master = master;
		this.neighborsAndDistance = new HashMap<>();
		this.neighborsAndDistance = neighborsAndDistance;
		//System.out.print("neighbors of "+id+" are ");
		for(Integer a : neighborsAndDistance.keySet()){
			//System.out.print(" "+a);
			status.put(a, "Unknown");
			Queue<Message> queue = new LinkedList<>();
			link.put(a, queue);
		}
	}
	
	//private long round = 0;
	
	public void setProcessNeighbors(Process p[]) {
		this.p = p;
	}
	
	@Override
	public void run() {
		//System.out.println("Process " + id + " started");
		// TODO Auto-generated method stub
		master.roundCompletionForProcess(id);
		//System.out.println("Process "+id+" ready");
		while(!processDone){
			//System.out.print(" Mesg is "+getMessageFromMaster()+"     ");
			if(getMessageFromMaster().equals("Initiate")){
				//System.out.println("Process "+id+" entered initiate");
				setMessageFromMaster("");
				//System.out.println("distance is "+distance);
				if(distance == 0){
					//System.out.println("in distance=0");
					sendMessages();
				}
				master.roundCompletionForProcess(id);	
			}else if (getMessageFromMaster().equals("StartRound")){
				/*
				 * Messages in the Queue are processed based on FIFO and then
				 * the current distance is sent to all the neighbors
				 */
				round++;
				//System.out.println("Process "+id+" entered start round");
				setMessageFromMaster("");
				boolean change = receiveMessages();
				//System.out.println("Change is "+change);
				if(change){
					sendMessages();
				}
				master.roundCompletionForProcess(id);
			}else if(getMessageFromMaster().equals("SendParent")){
				/* Master process collects parents
				 */
				
				//System.out.println("Process "+id+" entered send parent");
				setMessageFromMaster("");
				int weight;
				if(id == parent)
					weight = 0;
				else{
					//System.out.println("parent is "+parent);
					weight = neighborsAndDistance.get(parent);
				}
				master.assignParents(id,parent,weight);
				master.roundCompletionForProcess(id);
			}else if(getMessageFromMaster().equals("MasterDone")){
				// Terminating the Algorithm
				System.out.println("Process: " + id + "; Parent: " + parent + "; Distance from Source: " + distance);
				processDone = true;
			}else{
				//System.out.print("Entered else");
			}
		}
	}
	
	// Processing messages in the Queue and updating the current distance
	private boolean receiveMessages(){
		boolean flag = false;
		for (Integer neighbor : link.keySet()) {
			//System.out.println(neighbor);
			Message msg = modifyQueue(null, false, neighbor, false);
			//System.out.println(msg);
			while(msg != null){
				//System.out.println("message not null");
				long newDistance = msg.getCurDistance() + neighborsAndDistance.get(msg.getFromId());
				System.out.print("Process "+id+" New distance is "+newDistance+" and old distance "+msg.getCurDistance());
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
					}
				msg = new Message();
				msg = modifyQueue(null, false, neighbor, false);
			}
			if (acknowledge(id)) {
				//System.out.print("Acknowledged process is "+id+" parent is "+parent);
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
			//System.out.println(n);
			message.setTransmissionTime(random.nextInt(18));
			message.setSentRound(round);
			p[n].modifyQueue(message, true, this.id, false);

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
			Queue<Message> queue;
			//System.out.println(link.size());
			//System.out.println(link.get(1));
			if (insert) {
				queue = new LinkedList<Message>();
				//System.out.println(fromid);
				queue.add(msg);
				link.put(fromid, queue);
				//System.out.println(link.size()+" "+link.get(1));
				//System.out.println(link.get(fromid).peek().getTransmissionTime());
				return msg;
			} else {
				queue = link.get(fromid);
				//System.out.println(link.get(fromid).peek().getTransmissionTime());
				if (!queue.isEmpty()) {
					//System.out.println(fromid);
					Message m = queue.peek();
					//System.out.println((m.getSentRound() + m.getTransmissionTime()) + " "+round );
					if (round >= m.getSentRound() + m.getTransmissionTime()) {
						
						return queue.remove();
					} else{
						//System.out.println("in else");
						return null;
					}
						
				} else
					return null;
			}
		}
	}
	/* To get the recent signal send by the master process. 
	 * Inorder to execute the distributed systems synchronously
	 * */
	public String getMessageFromMaster(){
		return messageFromMaster;
	}
	
	/*
	 * To set the signal by the master process to processes. 
	 * Inorder to execute the distributed systems synchronously
	 * */
	public void setMessageFromMaster(String messageFromMaster) {
		this.messageFromMaster = messageFromMaster;
		//System.out.print("Entered setMessageFromMaster and msg of "+this.id +" is "+this.messageFromMaster+" ");
	}
	
}