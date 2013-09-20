package com.cloudera.sa.giraph.examples.ktrusses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class KTrussesEdgeWritable implements Writable {

	private int targetDegree;
	private int support;
	private boolean active;
	
	public KTrussesEdgeWritable() {}
	
	public KTrussesEdgeWritable(int targetDegree, int support, boolean active) {
		this.targetDegree = targetDegree;
		this.support = support;
		this.active = active;
	}
	
	public int getTargetDegree() {
		return targetDegree;
	}
	public void setTargetDegree(int targetDegree) {
		this.targetDegree = targetDegree;
	}
	public int getSupport() {
		return support;
	}
	public void setSupport(int support) {
		this.support = support;
	}
	public boolean isActive() {
		return active;
	}
	public void setActive(boolean active) {
		this.active = active;
	}

	public void readFields(DataInput in) throws IOException {
		targetDegree = in.readInt();
		support = in.readInt();
		active = in.readBoolean();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(targetDegree);
		out.writeInt(support);
		out.writeBoolean(active);
		
	}
	
	public String toString() {
		return "(" + targetDegree + "," + support + "," + active + ")";
	}
	
}
