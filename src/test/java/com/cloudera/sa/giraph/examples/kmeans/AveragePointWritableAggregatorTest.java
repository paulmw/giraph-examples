package com.cloudera.sa.giraph.examples.kmeans;

import org.junit.Assert;
import org.junit.Test;

public class AveragePointWritableAggregatorTest {

	@Test
	public void test() {
		double [][] data = {{0,1,1}, {1,0,1}, {1,1,0}};
		double [] twoThirds = {2.0d/3.0d, 2.0d/3.0d, 2.0d/3.0d};
		
		AveragePointWritableAggregator aggregator = new AveragePointWritableAggregator();
		for(double [] datum : data) {
			aggregator.aggregate(new PointWritable(datum));
		}
		
		Assert.assertArrayEquals(twoThirds, aggregator.getAggregatedValue().getData(), 0.0d);
	}

}
