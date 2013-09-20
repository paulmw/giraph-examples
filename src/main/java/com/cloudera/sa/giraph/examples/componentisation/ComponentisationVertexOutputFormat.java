package com.cloudera.sa.giraph.examples.componentisation;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ComponentisationVertexOutputFormat extends TextVertexOutputFormat<LongWritable, LongWritable, NullWritable> {

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
				Vertex<LongWritable, LongWritable, NullWritable, ?> vertex)
				throws IOException, InterruptedException {
			
			newKey.set(vertex.getId().toString());
			newValue.set(vertex.getValue().toString());
			
			getRecordWriter().write(newKey, newValue);
			
		}
		
	}
}
