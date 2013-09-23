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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class InputFormat extends TextVertexInputFormat<LongWritable, VertexState, EdgeState>{
	
	@Override
	public TextVertexReader createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new ComponentisationVertexReader();
	}

	public class ComponentisationVertexReader extends TextVertexReader {

		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}
		
		@Override
		public Vertex<LongWritable, VertexState, EdgeState, ?> getCurrentVertex() throws IOException, InterruptedException {
			Text line = getRecordReader().getCurrentValue();
			String[] parts = line.toString().split(" ");
			LongWritable id = new LongWritable(Long.parseLong(parts[0]));
		
			
			ArrayList<Edge<LongWritable, EdgeState>> edgeIdList = new ArrayList<Edge<LongWritable, EdgeState>>();
			 
			if(parts.length > 1) {
				for (int i = 1; i < parts.length; i++) {
					DefaultEdge<LongWritable, EdgeState> edge = new DefaultEdge<LongWritable, EdgeState>();
					edge.setTargetVertexId(new LongWritable(Long.parseLong(parts[i])));
					edge.setValue(new EdgeState(0,0,true));
					edgeIdList.add(edge);
				}
			}
			
		    Vertex<LongWritable, VertexState, EdgeState, ?> vertex = getConf().createVertex();
		    
		    vertex.initialize(id, new VertexState(), edgeIdList);
		    return vertex;
		}
	}
	
}
