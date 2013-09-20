package com.cloudera.sa.giraph.examples.ktrusses;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class KTrussesVertexOutputFormat extends TextVertexOutputFormat<LongWritable, KTrussesNodeWritable, KTrussesEdgeWritable> {

	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		return new KTrussesVertexWriter();
	}

	public class KTrussesVertexWriter extends TextVertexWriter {

		private Text line = new Text();
		private StringBuilder sb = new StringBuilder();
		
		public void writeVertex(Vertex<LongWritable, KTrussesNodeWritable, KTrussesEdgeWritable, ?> vertex) throws IOException, InterruptedException {
			sb.append("Node: " + vertex.getId());
			if(vertex.getValue().isInATruss()) {
				sb.append(", trussID: " + vertex.getValue().getTrussID());
			}
			line.set(sb.toString());
			getRecordWriter().write(null, line);
			sb.setLength(0);
		}
		
	}
}
