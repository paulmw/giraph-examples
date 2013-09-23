/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sa.giraph.examples.kmeans;

import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job 
{
	public static void main(String[] args) throws Exception {
	    
		if (args.length != 4) {
			System.out.println("KMeans Help:");
			System.out.println("Parameters: <numbersOfWorkers> <inputLocation> <outputLocation> <k>");
			System.out.println("Example: 1 inputFolder outputFolder 3");
			return;
		}
		
		String numberOfWorkers = args[0];
		String inputLocation = args[1];
		String outputLocation = args[2];
		int k = Integer.parseInt(args[3]);
		
	    GiraphJob bspJob = new GiraphJob(new Configuration(), Job.class.getName());
	    
	    bspJob.getConfiguration().setInt(Constants.K, k);
	    
	    bspJob.getConfiguration().setVertexClass(KMeansVertex.class);
	    bspJob.getConfiguration().setMasterComputeClass(MasterCompute.class);
	    bspJob.getConfiguration().setVertexInputFormatClass(InputFormat.class);
	    GiraphFileInputFormat.addVertexInputPath(bspJob.getConfiguration(), new Path(inputLocation));
	    
	    bspJob.getConfiguration().setVertexOutputFormatClass(OutputFormat.class);
	    
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
