package com.cloudera.sa.giraph.examples.ktrusses;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;

public class KTrussesMasterCompute extends DefaultMasterCompute {

	@Override
	public void initialize() throws InstantiationException,
	IllegalAccessException {
		registerAggregator(Constants.UPDATES, LongSumAggregator.class);
		registerPersistentAggregator(Constants.PHASE, WritableKTrussesPhaseAggregator.class);
	}

	@Override
	public void compute() {
		WritableKTrussesPhase phase = getAggregatedValue(Constants.PHASE);
		LongWritable updates = getAggregatedValue(Constants.UPDATES);
		if(phase.get() == null) {
			phase.set(KTrussesPhase.KCORE);

		} else if(phase.get() == KTrussesPhase.KCORE) {
			if(updates.get() == 0) {
				phase.set(KTrussesPhase.DEGREE);
			} else {
				phase.set(KTrussesPhase.KCORE);
			}
		} else if(phase.get() == KTrussesPhase.DEGREE) {
			phase.set(KTrussesPhase.QUERY_FOR_CLOSING_EDGES);
		} else if(phase.get() == KTrussesPhase.QUERY_FOR_CLOSING_EDGES) {
			phase.set(KTrussesPhase.FIND_TRIANGLES);
		} else if(phase.get() == KTrussesPhase.FIND_TRIANGLES) {
			phase.set(KTrussesPhase.TRUSSES);
		} else if(phase.get() == KTrussesPhase.TRUSSES) {
			if(updates.get() == 0) {
				phase.set(KTrussesPhase.COMPONENTISATION_1);
			} else {
				phase.set(KTrussesPhase.DEGREE);
			}
		} else if(phase.get() == KTrussesPhase.COMPONENTISATION_1) {
			phase.set(KTrussesPhase.COMPONENTISATION_2);
		} else if(phase.get() == KTrussesPhase.COMPONENTISATION_2) {
			if(updates.get() == 0) {
				phase.set(KTrussesPhase.OUTPUT);
			} else {
				phase.set(KTrussesPhase.COMPONENTISATION_2);
			}
		}
		setAggregatedValue(Constants.PHASE, phase);
		System.out.println("Superstep " + getSuperstep() + ", phase " + phase.get());
	}

}
