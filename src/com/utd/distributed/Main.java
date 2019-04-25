/**
 *
 * Main.java - Entry point for Distributed Algorithm for SynchBFS
 *
 *
 */

package com.utd.distributed;

import com.utd.distributed.master.Master;
import com.utd.distributed.process.Process;
import java.io.File;
import java.util.HashMap;
import java.util.Scanner;

public class Main {
	
	public static Process[] p;
	public static int ProcessCount;
	public static int source;
	
    public static void main(String[] args) {

        try {
            String filePath = "data.txt";
            System.out.println("-------Distributed Algorithm Asynchronous BFS Starts Here-------");
            File file = new File(filePath);
			Scanner sc = new Scanner(file);
			ProcessCount = sc.nextInt();
			System.out.println("The number of processes are "+ProcessCount);
			source = sc.nextInt();
			System.out.println(source+" is the source process");
			p = new Process[ProcessCount];
			HashMap<Integer,Integer> neighborsAndDistance;
			Master master = new Master(ProcessCount);
			int temp = 0;
			for(int i = 0; i < ProcessCount; i++){
				neighborsAndDistance = new HashMap<>();
				for(int j = 0; j < ProcessCount; j++){
					temp = sc.nextInt();
					if(temp != -1){
						neighborsAndDistance.put(j, temp);
					}
				}
				if(i == source)
					p[i] = new Process(i,neighborsAndDistance,master,i,0);
				else
					p[i] = new Process(i,neighborsAndDistance,master,-1,Long.MAX_VALUE);
			}
			
			master.setProcesses(p);
			Thread t = new Thread(master);
			t.start();
			for(int i = 0; i < ProcessCount; i++){
				Thread tempThread = new Thread(p[i]);
				tempThread.start();
			}
			sc.close();
        }
        catch (Exception ex)    {
            ex.printStackTrace();
        }
    }
}
