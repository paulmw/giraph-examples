package com.cloudera.sa.giraph.examples.componentisation;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;


public class ComponentisationVertex extends Vertex<LongWritable, LongWritable, NullWritable, LongWritable>{

	private LongWritable one = new LongWritable(1);
	
	@Override
	public void compute(Iterable<LongWritable> messages) throws IOException {

		boolean updated = false;
		
		ComponentisationWorkerContext workerContext = (ComponentisationWorkerContext) getWorkerContext();

		if(getSuperstep() == 0) {
			// In Superstep 0, we already know our neighbours state is equal to their id
			long lowestId = getId().get();
			for(Edge<LongWritable, NullWritable> edge: getEdges()) {
				lowestId = Math.min(lowestId, edge.getTargetVertexId().get());
			}
			if(lowestId < getId().get()) {
				getValue().set(lowestId);
				updated = true;
				workerContext.aggregate(Const.UPDATES_MADE_THIS_COMPUTE, one);
			}
		} else {
			// In all other supersteps we have to process messages to see if we should be updated
			long lowestValue = getValue().get();
			for(LongWritable message : messages) {
				lowestValue = Math.min(lowestValue, message.get());
			}
			if(lowestValue < getValue().get()) {
				getValue().set(lowestValue);
				updated = true;
				workerContext.aggregate(Const.UPDATES_MADE_THIS_COMPUTE, one);
			}
		}
		
		if(updated) {
			sendMessageToAllEdges(getValue());
		}
		voteToHalt();
		
	}
	
}
