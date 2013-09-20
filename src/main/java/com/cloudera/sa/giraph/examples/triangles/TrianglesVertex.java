package com.cloudera.sa.giraph.examples.triangles;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


public class TrianglesVertex extends Vertex<LongWritable, IntWritable, IntWritable, Message>{
	
	/*
	 * This method is used to compare two nodes by degree and name, in order to determine the
	 * most efficient way to find triangles.
	 */
	private boolean ordering(int degreeA, long idA, int degreeB, long idB) {
		if(degreeA < degreeB) {
			return true;
		}
		if(degreeA == degreeB) {
			if(idA < idB) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void compute(Iterable<Message> messages) throws IOException {

		System.out.println("Superstep " + getSuperstep() + ", node " + getId());
		
		// Phase 0 - compute degrees and send them out
		if(getSuperstep() == 0) {
			IntWritable degree = new IntWritable(getNumEdges());
			setValue(degree);
			sendMessageToAllEdges(new Message(getId(), degree));
		}

		// Phase 1 - annotate degrees onto edges
		if(getSuperstep() == 1) {
			for(Message message : messages) {
				setEdgeValue(message.getSource(), message.getDegree());
			}
		}

		// Phase 2 - Query neighbours about possible triangles
		if(getSuperstep() == 2) {
			int myDegree = getValue().get();
			if(myDegree > 1) {
				for(Edge<LongWritable, IntWritable> neighbourA : getEdges()) {
					int neighbourADegree = neighbourA.getValue().get();
					if(ordering(myDegree, getId().get(), neighbourADegree, neighbourA.getTargetVertexId().get())) {
						for(Edge<LongWritable, IntWritable> neighbourB : getEdges()) {
							int neighbourBDegree = neighbourB.getValue().get();
							if(ordering(neighbourADegree, neighbourA.getTargetVertexId().get(), neighbourBDegree, neighbourB.getTargetVertexId().get())) {
								System.out.println("I am node " + getId() + ", and I will ask node " + neighbourA.getTargetVertexId() + " whether it links to  " + neighbourB.getTargetVertexId());
								sendMessage(neighbourA.getTargetVertexId(), new Message(getId(), neighbourB.getTargetVertexId()));
							}
						}
					}	
				}
			}
			for(MutableEdge<LongWritable, IntWritable> edge : getMutableEdges()) {
				edge.setValue(new IntWritable(0));
			}
		}
		
		// Phase 3 - Report triangles found
		if(getSuperstep() == 3) {
			Set<Long> edges = new HashSet<Long>();
			for(MutableEdge<LongWritable, IntWritable> edge : getMutableEdges()) {
				edges.add(edge.getTargetVertexId().get());
			}
			for(Message message : messages) {
				System.out.println("I am node " + getId() + ", and I received this: \"" + message + "\"");
				if(edges.contains(message.getTriadA().get()) && edges.contains(message.getTriadB().get())) {
					System.out.println("I am node " + getId() + ", and I found the triangle (" + getId() + "," + message.getTriadA() + "," + message.getTriadB() + ")");
				}
			}
			voteToHalt();
		}

	}

}
