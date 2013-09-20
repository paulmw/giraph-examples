package com.cloudera.sa.giraph.examples.ktrusses;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;

public class MasterCompute extends DefaultMasterCompute {

	@Override
	public void initialize() throws InstantiationException,
	IllegalAccessException {
		registerAggregator(Constants.UPDATES, LongSumAggregator.class);
		registerPersistentAggregator(Constants.PHASE, PhaseWritableAggregator.class);
	}

	@Override
	public void compute() {
		PhaseWritable phase = getAggregatedValue(Constants.PHASE);
		LongWritable updates = getAggregatedValue(Constants.UPDATES);
		if(phase.get() == null) {
			phase.set(Phase.KCORE);

		} else if(phase.get() == Phase.KCORE) {
			if(updates.get() == 0) {
				phase.set(Phase.DEGREE);
			} else {
				phase.set(Phase.KCORE);
			}
		} else if(phase.get() == Phase.DEGREE) {
			phase.set(Phase.QUERY_FOR_CLOSING_EDGES);
		} else if(phase.get() == Phase.QUERY_FOR_CLOSING_EDGES) {
			phase.set(Phase.FIND_TRIANGLES);
		} else if(phase.get() == Phase.FIND_TRIANGLES) {
			phase.set(Phase.TRUSSES);
		} else if(phase.get() == Phase.TRUSSES) {
			if(updates.get() == 0) {
				phase.set(Phase.COMPONENTISATION_1);
			} else {
				phase.set(Phase.DEGREE);
			}
		} else if(phase.get() == Phase.COMPONENTISATION_1) {
			phase.set(Phase.COMPONENTISATION_2);
		} else if(phase.get() == Phase.COMPONENTISATION_2) {
			if(updates.get() == 0) {
				phase.set(Phase.OUTPUT);
			} else {
				phase.set(Phase.COMPONENTISATION_2);
			}
		}
		setAggregatedValue(Constants.PHASE, phase);
		System.out.println("Superstep " + getSuperstep() + ", phase " + phase.get());
	}

}
