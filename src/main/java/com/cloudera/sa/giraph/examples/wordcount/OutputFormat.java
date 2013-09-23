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

package com.cloudera.sa.giraph.examples.wordcount;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class OutputFormat extends TextVertexOutputFormat<Text, NullWritable, IntWritable> {

	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		return new ComponentisationVertexWriter();
	}

	public class ComponentisationVertexWriter extends TextVertexWriter {

		Text newKey = new Text();
		Text newValue = new Text();
		
		public void writeVertex(
				Vertex<Text, NullWritable, IntWritable, ?> vertex)
				throws IOException, InterruptedException {
			
			newKey.set(vertex.getId().toString());
			int count = 0;
			for(Edge<Text, IntWritable> edge : vertex.getEdges()) {
				count += edge.getValue().get();
			}
			newValue.set(count + "");
			
			getRecordWriter().write(newKey, newValue);
			
		}
		
	}
}
