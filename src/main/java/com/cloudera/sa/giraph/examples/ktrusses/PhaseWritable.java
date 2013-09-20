package com.cloudera.sa.giraph.examples.ktrusses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class PhaseWritable implements Writable {
	
	private Phase phase;
	
	public PhaseWritable() {}
	
	public PhaseWritable(Phase phase) {
		this.phase = phase;
	}

	public Phase get() {
		return phase;
	}

	public void set(Phase phase) {
		this.phase = phase;
	}

	public void readFields(DataInput in) throws IOException {
		boolean isNull = in.readBoolean();
		if(!isNull) {
			phase = WritableUtils.readEnum(in, Phase.class);
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(phase == null);
		if(phase != null) {
			WritableUtils.writeEnum(out, phase);	
		}
	}
	
}
