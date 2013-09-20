package com.cloudera.sa.giraph.examples.kcore;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class KCoreVertex extends Vertex<LongWritable, NullWritable, NullWritable, LongWritable>{

	private int k = -1;

	@Override
	public void compute(Iterable<LongWritable> messages) throws IOException {
		
		k = getConf().getInt("k", 2);

		for(LongWritable message : messages) {
			removeEdges(message);
		}
		
		if(getNumEdges() < k) {
			sendMessageToAllEdges(getId());
			for(Edge<LongWritable, NullWritable> edge : getEdges()) {
				removeEdges(edge.getTargetVertexId());
			}
			removeVertexRequest(getId());
		} else {
			voteToHalt();
		}
		
	}



}
