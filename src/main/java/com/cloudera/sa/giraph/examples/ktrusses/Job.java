package com.cloudera.sa.giraph.examples.ktrusses;

import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job {
	
	public static void main(String[] args) throws Exception {
	    
		if (args.length != 4) {
			System.out.println("KTrusses Help:");
			System.out.println("Parameters: <numbersOfWorkers> <inputLocation> <outputLocation> <k>");
			System.out.println("Example: 1 inputFolder outputFolder 4");
			return;
		}
		
		String numberOfWorkers = args[0];
		String inputLocation = args[1];
		String outputLocation = args[2];
		int k = Integer.parseInt(args[3]);
		
	    GiraphJob bspJob = new GiraphJob(new Configuration(), Job.class.getName());
	    
	    bspJob.getConfiguration().setVertexClass(KTrussVertex.class);
	    bspJob.getConfiguration().setVertexInputFormatClass(InputFormat.class);
	    GiraphFileInputFormat.addVertexInputPath(bspJob.getConfiguration(), new Path(inputLocation));
	    bspJob.getConfiguration().setMasterComputeClass(MasterCompute.class);
	    bspJob.getConfiguration().setVertexOutputFormatClass(OutputFormat.class);
	    
	    int minWorkers = Integer.parseInt(numberOfWorkers);
	    int maxWorkers = Integer.parseInt(numberOfWorkers);
	    bspJob.getConfiguration().setWorkerConfiguration(minWorkers, maxWorkers, 100.0f);

	    bspJob.getConfiguration().setInt(Constants.K, k);
	    
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
