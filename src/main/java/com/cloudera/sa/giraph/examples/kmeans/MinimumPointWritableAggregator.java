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

package com.cloudera.sa.giraph.examples.kmeans;

import org.apache.giraph.aggregators.Aggregator;

public class MinimumPointWritableAggregator implements Aggregator<PointWritable> {

	private PointWritable minimum = new PointWritable();
	
	public void aggregate(PointWritable value) {
		if(minimum.getDimensions() == 0) {
			minimum.setData(new double[value.getDimensions()]);
		}
		if(value.getDimensions() == 0) {
			return;
		}
		for(int i = 0; i < value.getDimensions(); i++) {
			minimum.getData()[i] = Math.min(minimum.getData()[i], value.getData()[i]);
		}
	}

	public PointWritable createInitialValue() {
		return new PointWritable();
	}

	public PointWritable getAggregatedValue() {
		return new PointWritable(minimum.getData());
	}

	public void setAggregatedValue(PointWritable value) {
		minimum.setData(value.getData().clone());
	}

	public void reset() {
		minimum.setData(new double[0]);
	}

}
