package com.utd.distributed.process;

public class Parent{
	public int weight;
	public int parent;
	
	public Parent(int parent, int weight){
		this.parent = parent;
		this.weight = weight;
	}
	
	public int getWeight() {
		return weight;
	}
	public void setWeight(int weight) {
		this.weight = weight;
	}
	public int getParent() {
		return parent;
	}
	public void setParent(int parent) {
		this.parent = parent;
	}
}
