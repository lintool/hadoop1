/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package test.hone.pagerank;

import edu.umd.cloud9.example.pagerank.RangePartitioner;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.util.Tool;
//import org.apache.log4j.Logger;
//import org.apache.hadoop.mapred.FileOutputFormat;

/**
 * <p>
 * Driver program for partitioning the graph. Command-line arguments are as
 * follows:
 * </p>
 * 
 * <ul>
 * 
 * <li>[inputDir]: input directory</li>
 * <li>[outputDir]: output directory</li>
 * <li>[numPartitions]: number of partitions</li>
 * <li>[useRange?]: 1 to user range partitioning or 0 otherwise</li>
 * <li>[nodeCount]: number of nodes in the graph</li>
 * 
 * </ul>
 * 
 * @author Jimmy Lin
 * @author Michael Schatz
 * 
 */
public class PartitionGraph {
	//private static final Logger sLogger = Logger.getLogger(PartitionGraph.class);

	public static void main(String[] args) throws Exception {
                PartitionGraph thisJob = new PartitionGraph();
                System.exit(thisJob.run(args));
		//int res = ToolRunner.run(new PartitionGraph(), args);
		//System.exit(res);
	}

	public PartitionGraph() {}

	private static int printUsage() {
		System.out.println("usage: [inputDir] [outputDir] [numPartitions] [useRange?] [nodeCount]");
	//	ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			printUsage();
			return -1;
		}

		String inPath = args[0];
		String outPath = args[1];
		int numParts = Integer.parseInt(args[2]);
		boolean useRange = Integer.parseInt(args[3]) != 0;
		int nodeCount = Integer.parseInt(args[4]);

//		sLogger.info("Tool name: PartitionGraph");
//		sLogger.info(" - inputDir: " + inPath);
//		sLogger.info(" - outputDir: " + outPath);
//		sLogger.info(" - numPartitions: " + numParts);
//		sLogger.info(" - useRange?: " + useRange);
//		sLogger.info(" - nodeCnt: " + nodeCount);

		Configuration conf = new Configuration();
		conf.setInt("NodeCount", nodeCount);

		Job job = new Job(conf, "Partition Graph " + numParts);
                //job.setMapperClass(PartitionGraph.class)
		//job.setJarByClass(PartitionGraph.class);

                //job.setReducerClass();
		job.setNumReduceTasks(numParts);

                //should be set in properties?
		//FileInputFormat.setInputPaths(job, new Path(inPath));
		//FileOutputFormat.setOutputPath(job, new Path(outPath));

		//job.setInputFormat(NonSplitableSequenceFileInputFormat.class);
		//job.setOutputFormat(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PageRankNode.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(PageRankNode.class);

		if (useRange) {
			job.setPartitionerClass(RangePartitioner.class);
		}
//this would get rid of the previous dir of this output
		//FileSystem.get(conf).delete(new Path(outPath), true);

		job.waitForCompletion(true);

		return 0;
	}
}
