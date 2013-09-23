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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class InputFormat extends TextEdgeInputFormat<Text, IntWritable>{

	@Override
	public EdgeReader<Text, IntWritable> createEdgeReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new WordCountEdgeReader();
	}

	public class WordCountEdgeReader extends TextEdgeReader {

		private Text uuid = new Text(UUID.randomUUID().toString());
		private Map<String, Integer> counts = new HashMap<String, Integer>();
		private Iterator<Map.Entry<String, Integer>> iterator;
		private Map.Entry<String, Integer> current;
		private Text source = new Text();

		private void processInput() throws IOException, InterruptedException {
			while(getRecordReader().nextKeyValue()) {
				Text line = getRecordReader().getCurrentValue();
				String[] parts = line.toString().split(" ");
				for(String part : parts) {
					part = part.trim();
					if(counts.containsKey(part)) {
						counts.put(part, counts.get(part) + 1);
					} else {
						counts.put(part, 1);
					}
				}
			}
			iterator = counts.entrySet().iterator();
		}


		@Override
		public boolean nextEdge() throws IOException, InterruptedException {
			if(iterator == null) {
				processInput();
			}
			if(iterator.hasNext()) {
				current = iterator.next();
				return true;
			}
			return false;
		}

		@Override
		public Text getCurrentSourceId() throws IOException, InterruptedException {
			source.set(current.getKey());
			return source;
		}

		@Override
		public Edge<Text, IntWritable> getCurrentEdge() throws IOException, InterruptedException {
			Edge<Text, IntWritable> edge = getConf().createEdge();
			edge.getTargetVertexId().set(uuid);
			edge.getValue().set(current.getValue());
			return edge;
		}

	}

}
