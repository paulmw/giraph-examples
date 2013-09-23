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

package com.cloudera.sa.giraph.examples.kcore;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/*
 * This algorithm reduces a graph to it's k-core. A k-core is a derived subgraph in which every vertex has a minimum degree
 * of k. This is performed by iteratively removing edges and nodes that don't satisfy this property, until no more edges are
 * removed.
 */
public class KCoreVertex extends Vertex<LongWritable, NullWritable, NullWritable, LongWritable>{

	private int k = 2;

	@Override
	public void compute(Iterable<LongWritable> messages) throws IOException {
		
		k = getConf().getInt(Constants.K, 2);

		for(LongWritable message : messages) {
			removeEdges(message);
		}
		
		if(getNumEdges() < k) {
			sendMessageToAllEdges(getId());
			for(Edge<LongWritable, NullWritable> edge : getEdges()) {
				removeEdges(edge.getTargetVertexId());
			}
			removeVertexRequest(getId());
		} else {
			voteToHalt();
		}
		
	}



}
