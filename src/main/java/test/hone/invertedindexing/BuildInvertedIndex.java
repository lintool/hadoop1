/*
 * Cloud9: A Hadoop toolkit for working with big data
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
package test.hone.invertedindexing;

import edu.umd.cloud9.util.fd.Object2IntFrequencyDistribution;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistributionEntry;
import edu.umd.cloud9.util.pair.PairOfObjectInt;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceMemoryOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class BuildInvertedIndex {

    //private static final Logger LOG = Logger.getLogger(BuildInvertedIndex.class);
    public static class MyMapper extends Mapper<LongWritable, Text, Text, PairOfInts> {

        private static final Text WORD = new Text();
        private final Object2IntFrequencyDistribution<String> COUNTS = new Object2IntFrequencyDistributionEntry<String>();
        PairOfInts pois;

        @Override
        public void setup(Mapper<LongWritable, Text, Text, PairOfInts>.Context context) {
            pois = new PairOfInts();
        }

        @Override
        public void map(LongWritable docno, Text doc, Context context) throws IOException, InterruptedException {
            String text = doc.toString();
            COUNTS.clear();

            String[] terms = text.split("\\s+");

            // First build a histogram of the terms.
            for (String term : terms) {
                if (term == null || term.length() == 0) {
                    continue;
                }

                COUNTS.increment(term);
            }

            // emit postings
            for (PairOfObjectInt<String> e : COUNTS) {
                WORD.set(e.getLeftElement());
                pois.set((int) docno.get(), e.getRightElement());
                context.write(WORD, pois);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, PairOfInts, Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> {

        private final static IntWritable DF = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<PairOfInts> values, Context context)
                throws IOException, InterruptedException {
            Iterator<PairOfInts> iter = values.iterator();
            ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();

            int df = 0;
            while (iter.hasNext()) {
                postings.add(iter.next().clone());
                df++;
            }

            Collections.sort(postings);
            DF.set(df);
            context.write(key, new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(DF, postings));
        }
    }

    private BuildInvertedIndex() {
    }

    private static int printUsage() {
        System.out.println("usage: [input-path] [output-path] [num-mappers]");
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

        String inputPath = "data/inputfiles";
//        String outputPath = args[1];
        //int mapTasks = Integer.parseInt(args[2]);
        int reduceTasks = 5;

//        LOG.info("Tool name: " + BuildInvertedIndex.class.getSimpleName());
//        LOG.info(" - input path: " + inputPath);
//        LOG.info(" - output path: " + outputPath);
//        LOG.info(" - num mappers: " + mapTasks);
//        LOG.info(" - num reducers: " + reduceTasks);

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJobName(BuildInvertedIndex.class.getSimpleName());
        //job.setJarByClass(BuildInvertedIndex.class);

        job.setInputDirectoryPath(inputPath);
        job.setIterationNo(0);

        job.setNumReduceTasks(reduceTasks);

        //FileInputFormat.setInputPaths(job, new Path(inputPath));
        //FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapInputKeyClass(LongWritable.class);
        job.setMapInputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairOfInts.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairOfWritables.class);
        job.setOutputFormatClass(SequenceMemoryOutputFormat.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setReducerRecordReader(TextPairOfIntsRecordReader.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Delete the output directory if it exists already.
        //Path outputDir = new Path(outputPath);
        //FileSystem.get(getConf()).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        BuildInvertedIndex buildInvertedIndex = new BuildInvertedIndex();
        buildInvertedIndex.run(args);
    }
}
