package org.apache.hadoop.test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Combiner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceMemoryOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.ZipfGenerator;
import org.apache.hadoop.util.math.Pi;

public class Workload_Simulator_Tibanna {

    static int numRed;

    private Workload_Simulator_Tibanna() {
    }

    public static class TokenizerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private final static IntWritable one = new IntWritable(1);
        //private final StringTokenizer itr = new StringTokenizer();
        private final Text word = new Text();
        private final IntWritable keyInt = new IntWritable();
        String[] tokenArray;
        private String str;
        private String interaction = "manyToOne";
        private String distributionToReducers = "biased";
        private final static String MANY2ONE = "manyToOne";
        private final static String ONE2ONE = "oneToOne";
        private final static String ONE2MANY = "oneToMany";
        private final static String BIASED = "biased";
        private final static String UNIFORM = "uniform";
        private final static String ZIPFIAN = "zipfian";
        private long timeInMillis = System.currentTimeMillis();
        private Random generator;
        private int intKey;
        private double alpha = 0.7;
        private int numReduceTasks;
        private Properties prop;
        private int lambda;
        private ZipfGenerator zipfGenerator;
        private int size;
        private double skew;
        private int payloadSize;
        private int manyToOneProb;
        cern.jet.random.engine.RandomEngine gen;
        private int[][] partitionedKeys;
        private final static int partitionedUniqueKeys = 10;
        private static int pi_decimals;

        public static int getPoisson(double lambda) {
            double L = Math.exp(-lambda);
            double p = 1.0;
            int k = 0;

            do {
                k++;
                p *= Math.random();
            } while (p > L);

            return k - 1;
        }

        public static int getPartition(IntWritable key, int numReduceTasks) {
            return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }

        public void setup(Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException {
            prop = new Properties();
            FileInputStream fis = new FileInputStream("config.properties");
            try {
                prop.load(fis);
            } catch (IOException ex) {
                Logger.getLogger(Workload_Simulator_Tibanna.class.getName()).log(Level.SEVERE, null, ex);
            }

            long timeInMillis = System.currentTimeMillis();
            generator = new Random(19580427 + timeInMillis);
            this.interaction = prop.getProperty("inputIntermediateInteraction");
            this.distributionToReducers = prop.getProperty("distributionToReducers");
            this.alpha = Double.parseDouble(prop.getProperty("alpha"));
            this.numReduceTasks = context.getJobObject().getNumReduceTasks();
            this.lambda = Integer.parseInt(prop.getProperty("lambda"));
            this.skew = Double.parseDouble(prop.getProperty("zipf_skew"));
            this.payloadSize = Integer.parseInt(prop.getProperty("payloadsize"));
            this.manyToOneProb = 100 / (int) ((Double.parseDouble(prop.getProperty("manyToOneProb"))) * 100);
            this.partitionedKeys = new int[this.numReduceTasks][partitionedUniqueKeys];
            this.pi_decimals = Integer.parseInt(prop.getProperty("cpuboundness_pi_decimals"));
            fis.close();

            if (distributionToReducers.equals(ZIPFIAN)) {
                int n = 0;
                for (int i = 0; i < this.numReduceTasks; ++i) {
                    for (int j = 0; j < partitionedUniqueKeys; ++j) {
                        n = generator.nextInt(10000);
                        for (intKey = n; intKey < 10000; ++intKey) {
                            keyInt.set(intKey);
                            if (getPartition(keyInt, numReduceTasks) == i) {
                                break;
                            }
                        }
                        partitionedKeys[i][j] = intKey;
                    }
                }
                zipfGenerator = new ZipfGenerator(this.numReduceTasks, this.skew);
            }
        }

        @Override
        public final void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " &'=;`\t\n\r\f()*_+$!][:,.\240\"");

            int n;
            int pkey;
            int zipfReducer;
            word.set(RandomStringUtils.random(this.payloadSize));

            while (itr.hasMoreTokens()) {
                //creates CPU boundness
                Pi.computePi(pi_decimals);

                str = itr.nextToken();
                if (interaction.equals(MANY2ONE)) {
                    intKey = generator.nextInt(manyToOneProb);
                    if (intKey == 0) {
                        if (distributionToReducers.equals(UNIFORM)) {
                            intKey = generator.nextInt(10000);
                        } else if (distributionToReducers.equals(BIASED)) {
                            if (generator.nextInt(10) < (alpha * 10)) {
                                intKey = generator.nextInt((10000 - 1) / numReduceTasks) * numReduceTasks;
                                keyInt.set(intKey);
                            } else {
                                int temp = generator.nextInt(numReduceTasks) + 1;
                                temp = (temp == numReduceTasks) ? temp - 1 : temp;
                                intKey = generator.nextInt((10000 - 1) / temp) * temp;
                                intKey = ((intKey % numReduceTasks) == 0) ? intKey - 1 : intKey;
                                keyInt.set(intKey);
                            }
                        } else {
                            zipfReducer = zipfGenerator.next();
                            pkey = generator.nextInt(partitionedUniqueKeys);
                            intKey = partitionedKeys[zipfReducer][pkey];
                        }

                        keyInt.set(intKey);
                        context.write(keyInt, word);
                    }

                } else if (interaction.equals(ONE2ONE)) {
                    if (distributionToReducers.equals(UNIFORM)) {
                        intKey = generator.nextInt(10000);
                        keyInt.set(intKey);
                    } else if (distributionToReducers.equals(BIASED)) {
                        if (generator.nextInt(10) < (alpha * 10)) {
                            intKey = generator.nextInt((10000 - 1) / numReduceTasks) * numReduceTasks;
                            keyInt.set(intKey);
                        } else {
                            int temp = generator.nextInt(numReduceTasks) + 1;
                            temp = (temp == numReduceTasks) ? temp - 1 : temp;
                            intKey = generator.nextInt((10000 - 1) / temp) * temp;
                            intKey = ((intKey % numReduceTasks) == 0) ? intKey - 1 : intKey;
                            keyInt.set(intKey);
                        }
                    } else {
                        zipfReducer = zipfGenerator.next();
                        pkey = generator.nextInt(partitionedUniqueKeys);
                        keyInt.set(partitionedKeys[zipfReducer][pkey]);
                    }

                    context.write(keyInt, word);
                } else if (interaction.equals(ONE2MANY)) {
                    n = getPoisson(lambda);
                    for (int i = 0; i < n; ++i) {
                        word.set(RandomStringUtils.random(this.payloadSize));
                        if (distributionToReducers.equals(UNIFORM)) {
                            intKey = generator.nextInt(10000);
                        } else if (distributionToReducers.equals(BIASED)) {
                            if (generator.nextInt(10) < (alpha * 10)) {
                                intKey = generator.nextInt((10000 - 1) / numReduceTasks) * numReduceTasks;
                                keyInt.set(intKey);
                            } else {
                                int temp = generator.nextInt(numReduceTasks) + 1;
                                temp = (temp == numReduceTasks) ? temp - 1 : temp;
                                intKey = generator.nextInt((10000 - 1) / temp) * temp;
                                intKey = ((intKey % numReduceTasks) == 0) ? intKey - 1 : intKey;
                                keyInt.set(intKey);
                            }
                        } else {
                            zipfReducer = zipfGenerator.next();
                            pkey = generator.nextInt(partitionedUniqueKeys);
                            intKey = partitionedKeys[zipfReducer][pkey];
                        }

                        keyInt.set(intKey);
                        context.write(keyInt, word);
                    }
                }
            }
        }
    }

    public static class IntSumCombiner extends Combiner<IntWritable, Text, IntWritable, IntWritable> {

        private static final IntWritable RESULT = new IntWritable();

        @Override
        public void reduce(final IntWritable key, final Iterable<Text> values, final Combiner.Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (Iterator<Text> i = values.iterator(); i.hasNext();) {
                //sum += (i.next()).get();
                sum += 1;
                i.remove();
                //val.get();
            }
            RESULT.set(sum);
            context.write(key, RESULT);
        }
    }

    /*
     * public static class IntSumReducer extends Reducer<IntWritable, Text,
     * Text, Text> {
     *
     * private static final IntWritable RESULT = new IntWritable();
     *
     * @Override public void reduce(final IntWritable key, final Iterable<Text>
     * values, final Context context) throws IOException, InterruptedException {
     * int sum = 0;
     *
     * //System.out.println("key: " + key); for (Text val : values) { //sum +=
     * val.get(); sum += 1; context.write(new
     * Text(RandomStringUtils.random(10)), val); } RESULT.set(sum); } }
     */
    private static List<Future<Object>> reduceOutput;

    public static class IntSumReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

        private static final IntWritable RESULT = new IntWritable();

        @Override
        public void reduce(final IntWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;

            //System.out.println("key: " + key); 
            for (Text val : values) {
                //sum += val.get();
                sum += 1;
            }

            RESULT.set(sum);

            context.write(key, RESULT);
        }
    }

    public static void main(final String[] args) {

        if (args.length != 2) {
            System.out.println("Args: inputDir numReducers");
        }

        final Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("config.properties"));


        } catch (IOException ex) {
            Logger.getLogger(Workload_Simulator_Tibanna.class.getName()).log(Level.SEVERE, null, ex);
        }

        //Logger.global.setLevel(Level.OFF);
        int noIterations = Integer.parseInt(prop.getProperty("noIterations"));
        for (int i = 0; i < noIterations; ++i) {

            final Configuration conf = new Configuration();
            final Job job = new Job(conf, "word count");

            conf.set("inputIntermediateInteraction", prop.getProperty("inputIntermediateInteraction"));
            conf.set("distributionToReducers", prop.getProperty("distributionToReducers"));
            conf.setFloat("alpha", Float.parseFloat(prop.getProperty("alpha")));
            conf.setInt("lambda", Integer.parseInt(prop.getProperty("lambda")));
            conf.setFloat("zipf_skew", Float.parseFloat(prop.getProperty("zipf_skew")));
            conf.setInt("payloadsize", Integer.parseInt(prop.getProperty("payloadsize")));
            job.setInputDirectoryPath(args[0]);
            job.setIterationNo(0);
            //job.setNumMapTasks(20);
            job.setNumReduceTasks(Integer.parseInt(args[1]));
            job.setMapInputKeyClass(LongWritable.class);
            job.setMapInputValueClass(Text.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setMapperClass(TokenizerMapper.class);
            /**
             * ** Iterative options ***
             */
            job.setFirstIterationInputFromDisk(
                    true);
            job.setInMemoryInput(
                    true);
            job.setMapInputFromReducer(
                    true);
            job.setIterationNo(i);

            job.setInMemoryOutput(
                    true);
            job.setNumMapTasks(
                    120);
            job.setIterationOutputToDisk(
                    false, noIterations);
            job.setMapInput(reduceOutput);
            reduceOutput = null;

            //job.setCombinerClass(IntSumCombiner.class);
            job.setReducerClass(IntSumReducer.class);
            job.setPartitionerClass(HashPartitioner.class);
            job.setOutputFormatClass(SequenceMemoryOutputFormat.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.waitForCompletion(
                    true);
            reduceOutput = job.getReduceOutput();
        }
    }
}
