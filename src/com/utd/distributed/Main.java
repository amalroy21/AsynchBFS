/**
 *
 * Main.java - Entry point for Distributed Algorithm for SynchBFS
 *
 *
 */

package com.utd.distributed;

import com.utd.distributed.master.Master;
import com.utd.distributed.process.Process;
import com.utd.distributed.util.ReadPropertyFile;

import java.util.Properties;

public class Main {
	
	public static Process[] p;
	public static int ProcessCount;
	public static int source;
	
    public static void main(String[] args) {

        try {
            String filePath = "config.properties";
            Properties prop = ReadPropertyFile.readProperties(filePath);
            
            System.out.println("-------Distributed Algorithm SynchBFS-------");
            ProcessCount = Integer.parseInt(prop.getProperty("numberofProcess"));
            System.out.println("No of Process : "+ProcessCount);
            int root = Integer.parseInt(prop.getProperty("rootNode"));
            System.out.println("Process "+ root + " is the root process");
            p = new Process[ProcessCount];
			Master master = new Master(ProcessCount,root);
			String[] edgeList = prop.getProperty("edgeList").split(",");
			int[][] neighbors = new int[ProcessCount][ProcessCount];
			
			for(int i = 0; i < ProcessCount; i++){
				for(int j = 0; j < ProcessCount; j++){
					neighbors[i][j] = Integer.parseInt(edgeList[i].substring(j, j+1));
				}
				p[i] = new Process(i,root,neighbors[i],master);
			}
			
			master.setProcesses(p);
			Thread t = new Thread(master);
			t.start();
			for(int i = 0; i < ProcessCount; i++){
				Thread tempThread = new Thread(p[i]);
				tempThread.start();
			}
        }
        catch (Exception ex)    {
            ex.printStackTrace();
        }
    }
}
