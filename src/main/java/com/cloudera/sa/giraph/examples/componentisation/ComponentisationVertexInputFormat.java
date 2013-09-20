package com.cloudera.sa.giraph.examples.componentisation;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ComponentisationVertexInputFormat extends TextVertexInputFormat<LongWritable, LongWritable, NullWritable>{
	
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
		public Vertex<LongWritable, LongWritable, NullWritable, ?> getCurrentVertex() throws IOException, InterruptedException {
			Text line = getRecordReader().getCurrentValue();
			String[] parts = line.toString().split(" ");
			LongWritable id = new LongWritable(Long.parseLong(parts[0]));
			LongWritable value = new LongWritable(id.get());
			
			ArrayList<Edge<LongWritable, NullWritable>> edgeIdList = new ArrayList<Edge<LongWritable, NullWritable>>();
			 
			if(parts.length > 1) {
				for (int i = 1; i < parts.length; i++) {
					DefaultEdge<LongWritable, NullWritable> edge = new DefaultEdge<LongWritable, NullWritable>();
					edge.setTargetVertexId(new LongWritable(Long.parseLong(parts[i])));
					edge.setValue(NullWritable.get());
					edgeIdList.add(edge);
				}
			}
			
		    Vertex<LongWritable, LongWritable, NullWritable, ?> vertex = getConf().createVertex();
		    
		    vertex.initialize(id, value, edgeIdList);
		    return vertex;
		}
	}
	
}
