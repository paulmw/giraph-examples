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
