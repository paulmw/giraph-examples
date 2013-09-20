package com.cloudera.sa.giraph.examples.componentisation;


import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;

public class ComponentisationMasterCompute extends DefaultMasterCompute {
	
	@Override
    public void compute() {
		
		LongWritable updatesMade = getAggregatedValue(Const.UPDATES_MADE_THIS_COMPUTE);
		
		System.out.println("linksMade:" + updatesMade);
		System.out.println("getTotalNumEdges():" + getTotalNumEdges());
		System.out.println("getTotalNumVertices():" + getTotalNumVertices());
    }

    @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
    	registerAggregator(Const.UPDATES_MADE_THIS_COMPUTE,
    	          LongSumAggregator.class);
    }
}
