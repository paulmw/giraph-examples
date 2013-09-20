package com.cloudera.sa.giraph.examples.ktrusses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class KTrussesNodeWritable implements Writable {

	private int degree;
	private long trussID;
	private boolean isInATruss;
	
	public KTrussesNodeWritable() {}
	
	public KTrussesNodeWritable(int degree, long trussID, boolean isInATruss) {
		this.degree = degree;
		this.trussID = trussID;
		this.isInATruss = isInATruss;
	}

	public int getDegree() {
		return degree;
	}

	public void setDegree(int degree) {
		this.degree = degree;
	}

	public long getTrussID() {
		return trussID;
	}

	public void setTrussID(long trussID) {
		this.trussID = trussID;
	}

	public boolean isInATruss() {
		return isInATruss;
	}

	public void setInATruss(boolean isInATruss) {
		this.isInATruss = isInATruss;
	}

	public void readFields(DataInput in) throws IOException {
		degree = in.readInt();
		trussID = in.readLong();
		isInATruss = in.readBoolean();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(degree);
		out.writeLong(trussID);
		out.writeBoolean(isInATruss);
	}
	
	
	
}
