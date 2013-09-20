package com.cloudera.sa.giraph.examples.componentisation;

import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ComponentisationJob 
{
	public static void main(String[] args) throws Exception {
	    
		if (args.length != 3) {
			System.out.println("Componentisation Help:");
			System.out.println("Parameters: Componentisation <numbersOfWorkers> <inputLocaiton> <outputLocation>");
			System.out.println("Example: Componentisation 1 inputFolder outputFolder");
			return;
		}
		
		String numberOfWorkers = args[0];
		String inputLocation = args[1];
		String outputLocation = args[2];
		
	    GiraphJob bspJob = new GiraphJob(new Configuration(), ComponentisationJob.class.getName());
	    
	    bspJob.getConfiguration().setVertexClass(ComponentisationVertex.class);
	    bspJob.getConfiguration().setVertexInputFormatClass(ComponentisationVertexInputFormat.class);
	    GiraphFileInputFormat.addVertexInputPath(bspJob.getConfiguration(), new Path(inputLocation));
	    
	    bspJob.getConfiguration().setVertexOutputFormatClass(ComponentisationVertexOutputFormat.class);
	    bspJob.getConfiguration().setWorkerContextClass(ComponentisationWorkerContext.class);
	    bspJob.getConfiguration().setMasterComputeClass(ComponentisationMasterCompute.class);
	    
	    int minWorkers = Integer.parseInt(numberOfWorkers);
	    int maxWorkers = Integer.parseInt(numberOfWorkers);
	    bspJob.getConfiguration().setWorkerConfiguration(minWorkers, maxWorkers, 100.0f);

	    FileOutputFormat.setOutputPath(bspJob.getInternalJob(),
	                                   new Path(outputLocation));
	    boolean verbose = true;
	    
	    if (bspJob.run(verbose)) {
	      System.out.println("Ended well");
	    } else {
	      System.out.println("Ended with Failure");
	    }

	}
	 
}
