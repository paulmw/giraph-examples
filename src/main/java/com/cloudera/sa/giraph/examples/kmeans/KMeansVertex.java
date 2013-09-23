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

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/*
 * This algorithm implements K-Means clustering. Each data point in the dataset becomes a vertex 
 * in the graph, and cluster centres are computed using custom aggregators.
 */
public class KMeansVertex extends Vertex<LongWritable, NodeState, NullWritable, LongWritable>{
	
	private final static LongWritable one = new LongWritable(1);
	
	@Override
	public void compute(Iterable<LongWritable> messages) throws IOException {
		// In the first superstep, we compute the ranges of the dimensions 
		if(getSuperstep() == 0) {
			aggregate(Constants.MAX, getValue().getPoint());
			aggregate(Constants.MIN, getValue().getPoint());
			return;
		} else {
			
			// If there were no cluster reassignments in the previous superstep, we're done.
			// (Other stopping criteria (not implemented here) could include a fixed number of
			// iterations, cluster centres that are not moving, or the Residual Sum of Squares
			// (RSS) is below a certain threshold.
			if(getSuperstep() > 1) {
				LongWritable updates = getAggregatedValue(Constants.UPDATES);
				if(updates.get() == 0) {
					voteToHalt();
					return;
				}
			}
			
			// If we're not stopping, we need to compute the closest cluster to this node
			int k = getConf().getInt(Constants.K, 3);
			PointWritable [] means = new PointWritable[k];
			int closest = -1;
			double closestDistance = Double.MAX_VALUE;
			for(int i = 0; i < k; i++) {
				means[i] = getAggregatedValue(Constants.POINT_PREFIX + i);
				double d = distance(getValue().getPoint().getData(), means[i].getData());
				if(d < closestDistance) {
					closestDistance = d;
					closest = i;
				}
			}
			
			// If the choice of cluster has changed, aggregate an update so the we recompute
			// on the next iteration.
			if(closest != getValue().getCluster()) {
				aggregate(Constants.UPDATES, one);
			}
			
			// Ensure that the closest cluster position is updated, irrespective of whether or
			// not the choice of cluster has changed.
			NodeState state = getValue();
			state.setCluster(closest);
			state.setClusterCentre(means[closest]);
			setValue(state);
			
			// Prepare the next iteration by aggregating this point into the closest cluster.
			aggregate(Constants.POINT_PREFIX + closest, getValue().getPoint());
		}

	}
	
	/*
	 * Standard Euclidean L2 distance metric
	 */
	private double distance(double [] a, double [] b) {
		if(a.length == 0 || b.length == 0) {
			return Double.POSITIVE_INFINITY;
		}
		if(a.length != b.length) {
			throw new IllegalArgumentException();
		}
		double result = 0;
		for(int i = 0; i < a.length; i++) {
			result += Math.pow(b[i] - a[i], 2);
		}
		return Math.sqrt(result);
	}
	
}
