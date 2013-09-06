package test.hone.kmeans;

import com.rits.cloning.Cloner;
import java.io.*;
import java.util.Map.Entry;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Combiner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceMemoryOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * MapReduce implementation of the KMeansClustering algorithm using Hadoop.
 *
 * @author Ashwin Kayyoor
 *
 */
public class MapReduceKMeans_Eff_Tibanna {

    public static final String KEY_PREFIX = "kmeans.centers.";
    private static final String dataPath = "data/kmeans";
    //private static final String[] dataPaths = {"data-1m-splits", "data-10m-splits", "data-100-splits"};
    private static final String[] dataPaths = {"data-1m-splits"};
    private static final String[] clusterPaths = {"centers/centers-10.txt", "centers/centers-100.txt", "centers/centers-1000.txt"};

    //private static List<DoublePoint> oldcenters;
//  private static final int MAX_ITERATIONS = 1000;
//  private static final double EPSILON = 1E-8;
    /**
     * Handles the Map phase of the algorithm. Takes the input and assigns it to
     * a cluster.
     */
    public static class Map extends Mapper<Text, Text, IntWritable, PairOfDoublePointInt> {

        private Job job;
        private IntWritable minIndexKey = new IntWritable(1);
        private Text textValue = new Text("");
        private PairOfDoublePointInt pDoublePointInt = new PairOfDoublePointInt();
        private DoublePoint point = new DoublePoint();
        private HashMap<Integer, DoublePoint> centersMap;
        private List<DoublePoint> centers = new ArrayList();
        //private static String outString;
        private String[] coords = new String[2];

        //private static List<DoublePoint> centers;
        /**
         * Get the centers from the configuration.
         */
        @Override
        public void setup(Mapper<Text, Text, IntWritable, PairOfDoublePointInt>.Context context) {
            //super.setup(context.getConfiguration());

            job = context.getJobObject();

            if (centersMap == null) {
                for (int i = 0; i < 10000; ++i) {
                    if (job.getVariable(i + "") != null) {
                        centers.add((DoublePoint) job.getVariable(i + ""));
                    } else {
                        break;
                    }
                }
                //centersMap = (HashMap<Integer, DoublePoint>) job.getVariable("centersMap");
                //centers = (List<DoublePoint>) job.getVariable("centers");
//                if (centers == null) {
//                    centers.addAll(centersMap.values());
//                }
            } else {
                centers = (List<DoublePoint>) job.getVariable("centers");
                if (centers == null) {
                    centers = new ArrayList<DoublePoint>();
                    centers.addAll(centersMap.values());
                }
            }

            //job.setVariable("centersMap", centersMap);
            //job.setVariable("centers", centers);
            //oldcenters = centers;
        }

        /**
         * Map the input to a cluster.
         */
        @Override
        public final void map(final Text key, final Text input, final Context context) throws IOException {

            StringTokenizer tok = new StringTokenizer(input.toString(), ",");
            int i = 0;
            while (tok.hasMoreTokens()) {
                coords[i++] = tok.nextToken();
            }
            // Parse the input in the format x,y.
            // coords = input.toString().split(",");
            // DoublePoint point = new DoublePoint(Double.valueOf((double) Integer.parseInt(coords[0].trim())), Double.valueOf((double) Integer.parseInt(coords[1].trim())));

            //point.setXY(Double.valueOf((double) Integer.parseInt(coords[0].trim())), Double.valueOf((double) Integer.parseInt(coords[1].trim())));
            point.setXY(Double.parseDouble(coords[0].trim()), Double.parseDouble(coords[1].trim()));

            double minDist = Double.MAX_VALUE;
            int minIndex = 0;
            int index = 0;

            // Assign the input to a cluster.
            double distance;
            for (DoublePoint center : centers) {
                distance = center.distance(point);
                if (distance < minDist) {
                    minDist = distance;
                    minIndex = index;
                }

                index++;
            }

            // Emit the output as key, value.
            //outString = point.getX() + "," + point.getY() + "," + 1;
            pDoublePointInt.set(point, 1);
            //textValue.set(outString);
            minIndexKey.set(minIndex);
            context.write(minIndexKey, pDoublePointInt);
        }

        public void close() {
            //centersMap = null;
            //centers = null;
//            if (job.getVariable("centersMap") != null) {
//                job.setVariable("centersMap", null);
//                job.setVariable("centers", null);
//            }
        }
    }

    /**
     * Combiner class that calculates partial sums for the reducer.
     */
    public static class Combine extends Combiner<IntWritable, Text, IntWritable, Text> {

        private final static Text TextOutput = new Text("");

        @Override
        public void reduce(final IntWritable key, final Iterable<Text> values, final Combiner.Context context) throws IOException, InterruptedException {
            // Track the partial sums and the count
            double x = 0;
            double y = 0;
            int count = 0;

            String[] coords;
            for (Iterator<Text> i = values.iterator(); i.hasNext();) {
                coords = i.next().toString().split(",");
                x += Double.parseDouble(coords[0]);
                y += Double.parseDouble(coords[1]);
                count += 1;
            }

            // Construct the value to be x dimension, y dimension, and count
            String outString = Double.toString(x) + "," + Double.toString(y) + "," + count;
            TextOutput.set(outString);
            context.write(key, TextOutput);
        }
    }

    /**
     * A reducer class that takes results from the combiner to construct the
     * input.
     */
    public static class Reduce extends Reducer<IntWritable, PairOfDoublePointInt, IntWritable, DoublePoint> {

        //private List<DoublePoint> centers;
        //private final static Text TextOutput = new Text("");
        private final static IntWritable empty = new IntWritable(-1);
        private final static DoublePoint doublePointOutput = new DoublePoint();
        private List<DoublePoint> centers;
        private HashMap<Integer, DoublePoint> centersMap;
        private HashMap<Integer, DoublePoint> oldCentersMap;
        private Job job;
        //private String[] partialSums = new String[3];
        //private static String outStr;

        /**
         * Get the centers from the configuration.
         */
        @Override
        public void setup(Reducer<IntWritable, PairOfDoublePointInt, IntWritable, DoublePoint>.Context context) {
            job = context.getJobObject();            
        }

        /**
         * Calculate the new averages for the clusters.
         */
        @Override
        public void reduce(IntWritable key, Iterable<PairOfDoublePointInt> values, final Context context) throws IOException, InterruptedException {
            // We receive the sums from the combiner, so we have to sum up those values and count.
            double x = 0;
            double y = 0;
            int count = 0;
            int ind = 0;
            
            PairOfDoublePointInt pDoublePointInt;

            for (Iterator<PairOfDoublePointInt> i = values.iterator(); i.hasNext();) {

                pDoublePointInt = i.next();
                x += pDoublePointInt.getKey().getX();
                y += pDoublePointInt.getKey().getY();
                count += pDoublePointInt.getValue();
            }

            x /= count;
            y /= count;

            DoublePoint doublePoint = new DoublePoint(x, y);
            job.setVariable(key.get() + "", doublePoint);
            context.write(empty, doublePoint);
        }

        @Override
        public void close() {
            //job.setVariable("centersMap", centersMap);
            //System.exit(-1);
        }
    }

    static int printUsage() {
        System.out.println("MapReduceKMeans <inputs> <clusters> <output>");
        //ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    private Object createJob(String jobName, String dataPath, String clusterPath, String outputPath, int numReducers, HashMap<Integer, DoublePoint> centersMap, List<DoublePoint> centers) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf);


        job.setJobName(jobName);

        // the keys are strings.
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoublePoint.class);
        job.setMapInputKeyClass(Text.class);
        job.setMapInputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PairOfDoublePointInt.class);
        job.setOutputFormatClass(SequenceMemoryOutputFormat.class);
        job.setIterationNo(0);
        job.setMapperClass(Map.class);
        //job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);
        job.setNumMapTasks(1);
        job.setNumReduceTasks(numReducers);
        job.setInputDirectoryPath(dataPath);
        job.setPartitionerClass(HashPartitioner.class);

        int ind = 0;
        for (DoublePoint dp : centers) {
            job.setVariable(ind + "", dp);
            ind++;
        }
//        job.setVariable("centersMap", centersMap);
//        job.setVariable("centers", centers);
  //      long start = System.currentTimeMillis();
        //JobClient.runJob(conf);
        job.waitForCompletion(true);
//        long end = System.currentTimeMillis();

        for (int i = 0; i < 10000; ++i) {
            if (job.getVariable(i + "") != null) {
                centersMap.put(i, (DoublePoint) job.getVariable(i + ""));
            } else {
                break;
            }
        }             

        return centersMap;
        //System.out.println("took " + ((end - start) / 1000) + " seconds");
    }

    public int run(String[] args) throws Exception {
        String jobName;
        String fullDataPath;
        String fullClusterPath;
        String outputPath;
        Cloner cloner = new Cloner();
        HashMap<Integer, DoublePoint> centersMap = new HashMap();
        HashMap<Integer, DoublePoint> oldCentersMap = new HashMap();

        // Create jobs varying data.
        // for (String path : dataPaths) {
        //jobName = "kmeans-" + path.split("\\.")[0];
        jobName = "kmeans-" + args[0];
        fullDataPath = args[0] + "/" + args[1];
        fullClusterPath = args[0] + "/Centers/" + args[2] + "/centers" + args[2];
        outputPath = "/user/kmeans/output/" + jobName;

        int i = 0;
        String[] coords;
        DoublePoint point;
        List<DoublePoint> centers = new ArrayList();
//        for (Iterator<String> itr = centersLines.iterator(); itr.hasNext();) {
//            coords = itr.next().split(",");
//            point.setXY(Double.parseDouble(coords[0]), Double.parseDouble(coords[1]));
//            centersMap.put(i, point);
//            centers.add(point);
//            i++;
//        }

        // Read the clusters file to generate the clusters.
        FileInputStream fstream = new FileInputStream(fullClusterPath);
        DataInputStream stream = new DataInputStream(fstream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String line;
        int index = 0;
        while ((line = reader.readLine()) != null) {
            //job.setVariable(KEY_PREFIX + index, line);
            point = new DoublePoint();
            coords = line.split(",");
            point.setXY(Double.parseDouble(coords[0]), Double.parseDouble(coords[1]));
            //centersMap.put(index, point);
            centers.add(point);
            index++;
        }

        //job.setVariable(KEY_PREFIX + "count", Integer.toString(index));
        // Write out the clusters
        stream.close();
        fstream.close();
        reader.close();

        for (i = 0; i < 100; ++i) {
            System.out.println("************************ Iteration: " + i + " ****************************");
            System.out.println(fullDataPath);
            centersMap = (HashMap<Integer, DoublePoint>) this.createJob(jobName, fullDataPath, fullClusterPath, outputPath, Integer.parseInt(args[3]), centersMap, centers);
            Set oldSet = oldCentersMap.entrySet();
            if (!oldSet.isEmpty()) {
                boolean bool = false;
                for (Entry<Integer, DoublePoint> oldentry : oldCentersMap.entrySet()) {
                    if (oldentry.getValue().getX() - centersMap.get(oldentry.getKey()).getX() != 0 || oldentry.getValue().getY() - centersMap.get(oldentry.getKey()).getY() != 0) {
                        bool = true;
                        break;
                    }
                }
                if (bool) {
                    oldCentersMap = cloner.deepClone(centersMap);
                } else {
                    break;
                }
            } else {
                oldCentersMap = cloner.deepClone(centersMap);
            }
        }
        
        return 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Args: dataInputDir datasize numCenters numReducers");
            System.exit(-1);
        }

        //int res = ToolRunner.run(new Configuration(), new MapReduceKMeans(), args);
        MapReduceKMeans_Eff_Tibanna mapredKmeans = new MapReduceKMeans_Eff_Tibanna();
        mapredKmeans.run(args);
        //System.exit(res);

    }
}