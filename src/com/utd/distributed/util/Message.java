package com.utd.distributed.util;

public class Message {
	private int fromId;
	private long sentRound;

	public int getFromId() {
		return fromId;
	}

	public void setFromId(int fromID) {
		this.fromId = fromID;
	}

	public long getSentRound() {
		return sentRound;
	}

	public void setSentRound(long sentRound) {
		this.sentRound = sentRound;
	}
}
