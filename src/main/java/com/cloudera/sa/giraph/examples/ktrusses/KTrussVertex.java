package com.cloudera.sa.giraph.examples.ktrusses;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/*
 * This algorithm finds trusses in a graph.
 * 
 * Trusses are found using a series of algorithm phases: finding the k-core, to deactivate edges that can never be part of a truss;
 * finding the active degree of each node and putting that on the edges; finding triangles by intelligently querying to find closing
 * edges of open triads; finding the truss support of each edge and repeating everything after k-core until no edges are deactivated.
 * Once the trusses are found
 * 
 *  finds trusses. Once the trusses are found, 
 * a final Componentisation step is used to label each truss with a unique name for visualisation.
 */
public class KTrussVertex extends Vertex<LongWritable, VertexState, EdgeState, Message>{

	private static final LongWritable ONE = new LongWritable(1);

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

	/*
	 * This returns the number of active edges this node has.
	 */
	private int getActiveDegree() {
		int degree = 0;
		for(Edge<LongWritable, EdgeState> edge : getEdges()) {
			if(edge.getValue().isActive()) {
				degree++;
			}
		}
		return degree;
	}
	
	/*
	 * This is a utility method to send messages to active nodes.
	 */
	private void sendMessageToAllActiveNodes(Message message) {
		for(Edge<LongWritable, EdgeState> edge : getEdges()) {
			if(edge.getValue().isActive()) {
				sendMessage(edge.getTargetVertexId(), message);
			}
		}
	}
	
	/*
	 * This a utility method to evaluate a triangle message and use that to increment the support on each edge
	 * that is a part of the triangle.
	 */
	private void incrementSupport(LongWritable nodeID) {
		if(!nodeID.equals(getId())) {
			EdgeState ev = getEdgeValue(nodeID);
			ev.setSupport(ev.getSupport() + 1);
			setEdgeValue(nodeID, ev);
		}
	}
	
	@Override
	public void compute(Iterable<Message> messages) throws IOException {

		// Retreive the current phase of processing which is set by the MasterCompute
		PhaseWritable writablePhase = getAggregatedValue(Constants.PHASE);
		Phase phase = writablePhase.get();

		int k = getContext().getConfiguration().getInt(Constants.K, 3);
		
		/*
		 * This phase reduces the graph to k-1 core, which reduces the amount of work to find trusses.
		 */
		if(phase == Phase.KCORE) {
			
			// Deactivate edges that can't be part of a truss because their degree is too low
			int degree = getActiveDegree();
			if(degree < k - 1) {
				for(Edge<LongWritable, EdgeState> edge : getEdges()) {
					if(edge.getValue().isActive()) {
						EdgeState ev = getEdgeValue(edge.getTargetVertexId());
						ev.setActive(false);
						setEdgeValue(edge.getTargetVertexId(), ev);
						aggregate(Constants.UPDATES, ONE);
					}
				}
			}
		}

		/*
		 * This phase determines the active degree of a node (the number of nodes I connect to
		 * after not considering edges which can't be part of a truss). The degree is used to make triangle
		 * finding more efficient in later phases.
		 */
		if(phase == Phase.DEGREE) {
			// Determine active degree, store it in the node state and send it to all active neighbours
			int degree = getActiveDegree();
			setValue(new VertexState(degree, -1, false));
			sendMessageToAllActiveNodes(new Message(getId(), new IntWritable(degree)));
		}

		/*
		 * This phase starts the process of finding triangles. Triangles could be found by any of the 
		 * three nodes, so to ensure we only find a triangle once, we pick one of the nodes to do the work.
		 * Since we can pick which node does the work, we pick the node with lowest degree (fewest links),
		 * which is efficient. If two nodes have the same degree, we pick the node with the lowest name. 
		 * 
		 * Triangles are found by asking one of the other nodes whether it connects to a third node. This
		 * is done with a double-for-loop over the edges, considering only active edges. Again, we send the
		 * query to the node that isn't the highest degree.
		 */

		if(phase == Phase.QUERY_FOR_CLOSING_EDGES) {

			//First, we need to process the degree information and store it on our edges.
			for(Message message : messages) {
				EdgeState edgeValue = getEdgeValue(message.getSource());
				if(edgeValue == null) {
					edgeValue = new EdgeState();
				}
				int targetDegree = message.getDegree().get();
				edgeValue.setTargetDegree(targetDegree);
				setEdgeValue(message.getSource(), edgeValue);
			}

			// Now we find the possible triangles and send a query message to each.
			int myDegree = getValue().getDegree();
			if(myDegree > 1) {
				for(Edge<LongWritable, EdgeState> neighbourA : getEdges()) {
					if(neighbourA.getValue().isActive()) {
						int neighbourADegree = neighbourA.getValue().getTargetDegree();
						if(ordering(myDegree, getId().get(), neighbourADegree, neighbourA.getTargetVertexId().get())) {
							for(Edge<LongWritable, EdgeState> neighbourB : getEdges()) {
								if(neighbourB.getValue().isActive()) {
									int neighbourBDegree = neighbourB.getValue().getTargetDegree();
									if(ordering(neighbourADegree, neighbourA.getTargetVertexId().get(), neighbourBDegree, neighbourB.getTargetVertexId().get())) {
										sendMessage(neighbourA.getTargetVertexId(), new Message(getId(), neighbourB.getTargetVertexId()));
									}
								}
							}
						}	
					}
				}
			}
		}

		/*
		 * In this phase, we process the queries about closing edges and find triangles if the are present. We then
		 * send details of the triangle (which is only found by one node) to all nodes in that triangle.
		 */
		if(phase == Phase.FIND_TRIANGLES) {

			// Cache my active neighbour's names
			Set<Long> edges = new HashSet<Long>();
			for(MutableEdge<LongWritable, EdgeState> edge : getMutableEdges()) {
				if(edge.getValue().isActive()) {
					edges.add(edge.getTargetVertexId().get());
				}
			}

			// Find triangles and report them to participating nodes. 
			for(Message message : messages) {
				if(edges.contains(message.getTriadA().get()) && edges.contains(message.getTriadB().get())) {
					Message triangle = new Message(getId(), message.getTriadA(), message.getTriadB());
					sendMessage(getId(), triangle);
					sendMessage(message.getTriadA(), triangle);
					sendMessage(message.getTriadB(), triangle);
				}
			}
		}

		/*
		 * In this phase, the node processes the triangle reports and uses that to build a picture of how
		 * many triangles use each edge - this is known as the "support". We then use that to determine which edges
		 * are part of a truss by having enough support, and deactivating those that don't. Once this phase is complete,
		 * we evaluate (in the MasterCompute) whether or not the graph changed. If it did we iterate again, starting by
		 * determining the active degree, otherwise we continue.
		 * 
		 */
		if(phase == Phase.TRUSSES) {

			// Clear out support values from previous iterations
			for(Edge<LongWritable, EdgeState> edge : getEdges()) {
				EdgeState ev = getEdgeValue(edge.getTargetVertexId());
				ev.setSupport(0);
				setEdgeValue(edge.getTargetVertexId(), ev);
			}

			// Build a picture of the support of each edge
			for(Message message : messages) {
				incrementSupport(message.getTriangleA());
				incrementSupport(message.getTriangleB());
				incrementSupport(message.getTriangleC());
			}

			/*
			 * Go through our edges, marking any that don't have enough support as inactive. We also track how
			 * many edges we deactivated.
			 */
			for(Edge<LongWritable, EdgeState> edge : getEdges()) {
				if(edge.getValue().isActive()) {
					if(edge.getValue().getSupport() < k - 2) {
						EdgeState ev = getEdgeValue(edge.getTargetVertexId());
						ev.setActive(false);
						ev.setSupport(0);
						setEdgeValue(edge.getTargetVertexId(), ev);
					}
				}
			}
		}

		/*
		 * This is the first componentisation phase. At this point, we know our neighbour's trussID
		 * because it is initialised to be the same as their ID, so we use that to determine the lowest
		 * local truss ID. Once that is found, we broadcast it in order to let the value propagate.
		 */
		if(phase == Phase.COMPONENTISATION_1) {
			
			boolean updated = false;
			boolean isInATruss = false;
			
			// Determine whether the current node is in a truss or not.
			for(Edge<LongWritable, EdgeState> edge : getEdges()) {
				if(edge.getValue().isActive()) {
					isInATruss = true;
					break;
				}
			}

			if(isInATruss) {
				// Determine lowest Truss ID
				long lowestId = getId().get();
				for(Edge<LongWritable, EdgeState> edge : getEdges()) {
					if(edge.getValue().isActive()) {
						lowestId = Math.min(lowestId, edge.getTargetVertexId().get());
					}
				}
				if(lowestId <= getId().get()) {
					getValue().setTrussID(lowestId);
					getValue().setInATruss(true);
					updated = true;
				}

				// Broadcast the lowest truss ID if we were updated with it
				if(updated) {
					sendMessageToAllActiveNodes(new Message(new LongWritable(getValue().getTrussID())));
				}
			}
		}

		/*
		 * This is the second phase of the componentisation, in which the lowest truss ID received is propagated.
		 * If the truss ID is lowered, we again rebroadcast it and stay in this phase until there are no more updates.
		 */
		if(phase == Phase.COMPONENTISATION_2) {
			// Determine lowest Truss ID
			long lowestValue = getValue().getTrussID();
			for(Message message : messages) {
				lowestValue = Math.min(lowestValue, message.getTrussID().get());
			}
			boolean updated = false;
			if(lowestValue < getValue().getTrussID()) {
				getValue().setTrussID(lowestValue);
				aggregate(Constants.UPDATES, ONE);
				updated = true;

			}
			
			// Broadcast the lowest truss ID if we were updated with it
			if(updated) {
				sendMessageToAllActiveNodes(new Message(new LongWritable(getValue().getTrussID())));
			}
		}

		/*
		 * And we're done :)
		 */
		if(phase == Phase.OUTPUT) {
			voteToHalt();
		}
	}



}
