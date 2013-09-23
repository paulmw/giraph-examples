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

import java.util.Random;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;

public class MasterCompute extends DefaultMasterCompute {

	private int k;
	private Random r;
	
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		r = new Random();
		registerPersistentAggregator(Constants.MIN, MinimumPointWritableAggregator.class);
		registerPersistentAggregator(Constants.MAX, MaximumPointWritableAggregator.class);
		registerAggregator(Constants.UPDATES, LongSumAggregator.class);
		k = getConf().getInt(Constants.K, 0);
		
		for(int i = 0; i < k; i++) {
			registerAggregator(Constants.POINT_PREFIX + i, AveragePointWritableAggregator.class);
		}
	}

	@Override
	public void compute() {
		if(getSuperstep() == 1) {
			PointWritable min = getAggregatedValue(Constants.MIN);
			PointWritable max = getAggregatedValue(Constants.MAX);
			for(int i = 0; i < k; i++) {
				PointWritable p = random(min.getData(), max.getData());
				setAggregatedValue(Constants.POINT_PREFIX + i, p);
			}
		}
	}
	
	private PointWritable random(double [] min, double [] max) {
		if(min.length != max.length) {
			throw new IllegalArgumentException();
		}
		double [] randomData = new double[min.length];
		for(int i = 0; i < randomData.length; i++) {
			randomData[i] = random(min[i], max[i]);
		}
		return new PointWritable(randomData);
	}

	private double random(double min, double max) {
		double x = r.nextDouble();
		double range = max - min;
		return (x * range) + min;
	}
}