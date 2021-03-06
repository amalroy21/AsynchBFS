package com.utd.distributed.util;

public class Message {
	
	private int fromId;
	private long sentRound;
	private long transmissionTime;
	private long curDistance;
	private boolean isDummy;

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

	public long getTransmissionTime() {
		return transmissionTime;
	}

	public void setTransmissionTime(long transmissionTime) {
		this.transmissionTime = transmissionTime;
	}

	public long getCurDistance() {
		return curDistance;
	}

	public void setCurDistance(long curDistance) {
		this.curDistance = curDistance;
	}

	public boolean isDummy() {
		return isDummy;
	}

	public void setDummy(boolean isDummy) {
		this.isDummy = isDummy;
	}
	
	@Override
	public String toString() {
		return "Message [fromId=" + fromId + ", sentRound=" + sentRound + ", transmissionTime=" + transmissionTime
				+ ", curDistance=" + curDistance + ", isDummy=" + isDummy + "]";
	}
}
