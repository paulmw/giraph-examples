package com.cloudera.sa.giraph.examples.kcore;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class OutputFormat extends TextVertexOutputFormat<LongWritable, LongWritable, NullWritable> {

	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {

		return new KCoreVertexWriter();
	}

	public class KCoreVertexWriter extends TextVertexWriter {

		private Text line = new Text();
		private StringBuilder sb = new StringBuilder();

		public void writeVertex(Vertex<LongWritable, LongWritable, NullWritable, ?> vertex) throws IOException, InterruptedException {

			sb.append(vertex.getId());
			for(Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
				sb.append(" ");
				sb.append(edge.getTargetVertexId());
			}
			line.set(sb.toString());

			getRecordWriter().write(null, line);
			sb.setLength(0);
		}

	}
}
