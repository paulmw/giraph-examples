package com.cloudera.sa.giraph.examples.wordcount;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/*
 * Srsly?
 */
public class WordCountVertex extends Vertex<Text, NullWritable, IntWritable, NullWritable>{
	
	@Override
	public void compute(Iterable<NullWritable> messages) throws IOException {
		voteToHalt();
	}
	
}
