package com.cloudera.sa.giraph.examples.kmeans;

import org.junit.Assert;
import org.junit.Test;

public class MinimumPointWritableAggregatorTest {

	@Test
	public void test() {
		double [][] data = {{0,1,1}, {1,0,1}, {1,1,0}};
		double [] zeroes = {0,0,0};
		
		MinimumPointWritableAggregator aggregator = new MinimumPointWritableAggregator();
		for(double [] datum : data) {
			aggregator.aggregate(new PointWritable(datum));
		}
		
		Assert.assertArrayEquals(zeroes, aggregator.getAggregatedValue().getData(), 0.0d);
	}

}
