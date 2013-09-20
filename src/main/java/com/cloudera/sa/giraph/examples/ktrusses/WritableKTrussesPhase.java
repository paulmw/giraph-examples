package com.cloudera.sa.giraph.examples.ktrusses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class WritableKTrussesPhase implements Writable {
	
	private KTrussesPhase phase;
	
	public WritableKTrussesPhase() {}
	
	public WritableKTrussesPhase(KTrussesPhase phase) {
		this.phase = phase;
	}

	public KTrussesPhase get() {
		return phase;
	}

	public void set(KTrussesPhase phase) {
		this.phase = phase;
	}

	public void readFields(DataInput in) throws IOException {
		boolean isNull = in.readBoolean();
		if(!isNull) {
			phase = WritableUtils.readEnum(in, KTrussesPhase.class);
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(phase == null);
		if(phase != null) {
			WritableUtils.writeEnum(out, phase);	
		}
	}
	
}
