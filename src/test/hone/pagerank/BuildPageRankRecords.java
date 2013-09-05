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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.lib.output.SequenceFileOutputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.ArrayListOfIntsWritable;
//import java.util.logging.Level;
//import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
//import test.mapreduce4j.WordCount;

/**
 * <p> Driver program that takes a plain-text encoding of a directed graph and
 * builds corresponding Hadoop structures for representing the graph.
 * Command-line parameters are as follows: </p>
 *
 * <ul> <li>[inputDir]: input directory</li> <li>[outputDir]: output director
 * y</li> <li>[numNodes]: number of nodes in the graph</li> </ul>
 *
 * @author Jimmy Lin
 * @author Michael Schatz s
 */
public class BuildPageRankRecords {

    // private static final Logger LOG = Logger.getLogger(BuildPageRankRecords.class);
    private static final String NODE_CNT_FIELD = "node.cnt";

    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, PageRankNode> {

        private final IntWritable nid = new IntWritable(); //nodeid
        private final PageRankNode node = new PageRankNode(); //node

        @Override
        public void setup(Mapper<LongWritable, Text, IntWritable, PageRankNode>.Context context) {
            int n = context.getConfiguration().getInt(NODE_CNT_FIELD, 0);
            if (n == 0) {
                throw new RuntimeException(NODE_CNT_FIELD + " cannot be 0!");
            }
            node.setType(PageRankNode.Type.Complete);
            node.setPageRank((float) -StrictMath.log(n));
        }

        @Override
        public void map(LongWritable key, Text t, Context context)
                throws IOException, InterruptedException {
            String[] arr = t.toString().trim().split("\\s+");

            nid.set(Integer.parseInt(arr[0]));
            if (arr.length == 1) {
                node.setNodeId(Integer.parseInt(arr[0]));
                node.setAdjacencyList(new ArrayListOfIntsWritable());

            } else {
                node.setNodeId(Integer.parseInt(arr[0]));

                int[] neighbors = new int[arr.length - 1];
                for (int i = 1; i < arr.length; i++) {
                    neighbors[i - 1] = Integer.parseInt(arr[i]);
                }

                node.setAdjacencyList(new ArrayListOfIntsWritable(neighbors));
            }

            //context.getCounter("graph", "numNodes").increment(1);
            //context.getCounter("graph", "numEdges").increment(arr.length - 1);

            if (arr.length > 1) {
                //context.getCounter("graph", "numActiveNodes").increment(1);
            }

            context.write(nid, node);
        }
    }

    public BuildPageRankRecords() {
    }

    private static int printUsage() {
        System.out.println("usage: [inputDir] [outputDir] [numNodes]");
        //ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    /**
     * Runs this tool.
     */
    public int run(String[] args) throws Exception {
//        if (args.length != 3) {
//            printUsage();
//            return -1;
//        }

//        String inputPath = args[0];
//        String outputPath = args[1];
        String inputPath = "data/wikipedia-graph";
        String outputPath = "data/pagerank-temp/iter0000/pagerankrecords";

//        int n = Integer.parseInt(args[2]);
        int n = 9;

//		LOG.info("Tool name: BuildPageRankRecords");
//		LOG.info(" - inputDir: " + inputPath);
//		LOG.info(" - outputDir: " + outputPath);
//		LOG.info(" - numNodes: " + n);

        Configuration conf = new Configuration();
        conf.setInt(NODE_CNT_FIELD, n);
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

        Job job = new Job(conf, "BuildPageRankRecords");
        job.setMapperClass(MyMapper.class);
        //job.setJarByClass(BuildPageRankRecords.class);

        job.setIterationOutputToDisk(true, 0);
        job.setIterationNo(0);
        
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumMapTasks(1);
        job.setNumReduceTasks(0);

        //job.addInput(new File(inputPath));
        job.setInputDirectoryPath(inputPath);
        TextInputFormat.addInputPath(job, new Path(inputPath));
        SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));

        //job.setOutputFile(new File(outputPath));
        //FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PageRankNode.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PageRankNode.class);

        //job.setMapperClass(MyMapper.class);

        // Delete the output directory if it exists already.
//		FileSystem.get(conf).delete(new Path(outputPath), true);

        job.waitForCompletion(true);

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the
     * <code>ToolRunner</code>.
     */
    public static void main(String[] args) throws Exception {

        final BuildPageRankRecords thisJob = new BuildPageRankRecords();
        System.exit(thisJob.run(args));
    }
}
