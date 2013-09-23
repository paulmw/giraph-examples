package com.cloudera.sa.giraph.examples.kmeans;

import org.junit.Assert;
import org.junit.Test;

public class MaximumPointWritableAggregatorTest {

	@Test
	public void test() {
		double [][] data = {{0,1,1}, {1,0,1}, {1,1,0}};
		double [] ones = {1,1,1};
		
		MaximumPointWritableAggregator aggregator = new MaximumPointWritableAggregator();
		for(double [] datum : data) {
			aggregator.aggregate(new PointWritable(datum));
		}
		
		Assert.assertArrayEquals(ones, aggregator.getAggregatedValue().getData(), 0.0d);
	}

}
