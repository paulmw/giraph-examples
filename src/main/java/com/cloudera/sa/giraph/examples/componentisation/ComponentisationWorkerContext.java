package com.cloudera.sa.giraph.examples.componentisation;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.LongWritable;

public class ComponentisationWorkerContext extends WorkerContext {

	private long lastUpdatesMade = 0;
    private long updatesMadeThisCompute = 0;

	
	@Override
	public void preApplication() throws InstantiationException,
			IllegalAccessException {
	}

	@Override
	public void postApplication() {
		
	}

	@Override
	public void preSuperstep() {
		System.out.println("PreSuperStep:" + ((LongWritable)getAggregatedValue(Const.UPDATES_MADE_THIS_COMPUTE)).get());
		updatesMadeThisCompute = ((LongWritable)getAggregatedValue(Const.UPDATES_MADE_THIS_COMPUTE)).get();
		
	}

	@Override
	public void postSuperstep() {
		System.out.println("PostSuperStep:" + ((LongWritable)getAggregatedValue(Const.UPDATES_MADE_THIS_COMPUTE)).get());
		lastUpdatesMade = updatesMadeThisCompute;
	}
	
	public long getLinksMadeThisCompute() {
		return updatesMadeThisCompute;	
	}
	
	public long getLastLinksMade() {
		return lastUpdatesMade;
	}
	


}
