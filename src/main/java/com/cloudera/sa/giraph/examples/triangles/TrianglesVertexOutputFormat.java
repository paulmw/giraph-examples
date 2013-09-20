package com.cloudera.sa.giraph.examples.triangles;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TrianglesVertexOutputFormat extends TextVertexOutputFormat<LongWritable, IntWritable, IntWritable> {

	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		return new TrianglesVertexWriter();
	}

	public class TrianglesVertexWriter extends TextVertexWriter {

		private Text line = new Text();
		private StringBuilder sb = new StringBuilder();
		
		public void writeVertex(
				Vertex<LongWritable, IntWritable, IntWritable, ?> vertex)
				throws IOException, InterruptedException {
			
			sb.append(vertex.getId());
			for(Edge<LongWritable, IntWritable> edge : vertex.getEdges()) {
				sb.append(" ");
				sb.append(edge.getTargetVertexId() + ":" + edge.getValue());
			}
			line.set(sb.toString());

			getRecordWriter().write(null, line);
			sb.setLength(0);
			
		}
		
	}
}
