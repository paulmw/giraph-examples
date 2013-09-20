package com.cloudera.sa.giraph.examples.kcore;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/*
 * This algorithm reduces a graph to it's k-core. A k-core is a derived subgraph in which every vertex has a minimum degree
 * of k. This is performed by iteratively removing edges and nodes that don't satisfy this property, until no more edges are
 * removed.
 */
public class KCoreVertex extends Vertex<LongWritable, NullWritable, NullWritable, LongWritable>{

	private int k = 2;

	@Override
	public void compute(Iterable<LongWritable> messages) throws IOException {
		
		k = getConf().getInt(Constants.K, 2);

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
