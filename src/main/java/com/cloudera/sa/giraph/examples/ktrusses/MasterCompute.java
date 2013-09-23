/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
	}

}
