package com.cloudera.sa.giraph.examples.triangles;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TrianglesVertexInputFormat extends TextVertexInputFormat<LongWritable, IntWritable, IntWritable>{
	
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
		public Vertex<LongWritable, IntWritable, IntWritable, ?> getCurrentVertex() throws IOException, InterruptedException {
			Text line = getRecordReader().getCurrentValue();
			String[] parts = line.toString().split(" ");
			LongWritable id = new LongWritable(Long.parseLong(parts[0]));
		
			
			ArrayList<Edge<LongWritable, IntWritable>> edgeIdList = new ArrayList<Edge<LongWritable, IntWritable>>();
			 
			if(parts.length > 1) {
				for (int i = 1; i < parts.length; i++) {
					DefaultEdge<LongWritable, IntWritable> edge = new DefaultEdge<LongWritable, IntWritable>();
					edge.setTargetVertexId(new LongWritable(Long.parseLong(parts[i])));
					edge.setValue(new IntWritable(0));
					edgeIdList.add(edge);
				}
			}
			
		    Vertex<LongWritable, IntWritable, IntWritable, ?> vertex = getConf().createVertex();
		    
		    vertex.initialize(id, new IntWritable(0), edgeIdList);
		    return vertex;
		}
	}
	
}
