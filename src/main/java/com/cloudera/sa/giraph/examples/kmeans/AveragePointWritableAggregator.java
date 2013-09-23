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

public class AveragePointWritableAggregator implements Aggregator<PointWritable> {

	private PointWritable sum = new PointWritable();
	private int count = 0;
	
	public void aggregate(PointWritable value) {
		if(sum.getDimensions() == 0) {
			sum.setData(new double[value.getDimensions()]);
		}
		if(value.getDimensions() == 0) {
			return;
		}
		for(int i = 0; i < value.getDimensions(); i++) {
			sum.getData()[i] = sum.getData()[i] + value.getData()[i];
		}
		count++;
	}

	public PointWritable createInitialValue() {
		return new PointWritable();
	}

	public PointWritable getAggregatedValue() {
		double [] data = sum.getData().clone();
		for(int i = 0; i < data.length; i++) {
			data[i] /= count;
		}
		return new PointWritable(data);
	}

	public void setAggregatedValue(PointWritable value) {
		sum.setData(value.getData().clone());
		count = value.getDimensions() == 0 ? 0 : 1;
	}

	public void reset() {
		sum.setData(new double[0]);
		count = 0;
	}


}
