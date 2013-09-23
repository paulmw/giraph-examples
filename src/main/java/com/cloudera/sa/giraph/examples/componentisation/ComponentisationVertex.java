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

package com.cloudera.sa.giraph.examples.componentisation;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/*
 * This algorithm finds the connected components of a graph. This is done by giving every vertex a unique label,
 * and propagating that label via connecting edges in order to find the lowest label. It is iterative, in that if a
 * node receives a new lowest label, it also broadcasts the new label to it's own neighbours.
 * 
 * At the end of each superstep, every node votes to halt, but if a message arrives in the next superstep, it
 * reactivates. Once no more label update messages are sent and all nodes are inactive, we stop.
 */
public class ComponentisationVertex extends Vertex<LongWritable, LongWritable, NullWritable, LongWritable>{
	
	@Override
	public void compute(Iterable<LongWritable> messages) throws IOException {

		boolean updated = false;

		if(getSuperstep() == 0) {
			// In Superstep 0, we already know our neighbour's state is equal to their id (by definition),
			// so we cheat a little by using that knowledge, saving a superstep.
			long lowestId = getId().get();
			for(Edge<LongWritable, NullWritable> edge: getEdges()) {
				lowestId = Math.min(lowestId, edge.getTargetVertexId().get());
			}
			if(lowestId < getId().get()) {
				getValue().set(lowestId);
				updated = true;
			}
		} else {
			// In all other supersteps we have to process messages to see if we should be updated
			long lowestValue = getValue().get();
			for(LongWritable message : messages) {
				lowestValue = Math.min(lowestValue, message.get());
			}
			if(lowestValue < getValue().get()) {
				getValue().set(lowestValue);
				updated = true;
			}
		}
		
		if(updated) {
			sendMessageToAllEdges(getValue());
		}
		voteToHalt();
		
	}
	
}
