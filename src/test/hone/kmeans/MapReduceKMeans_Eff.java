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
public class MapReduceKMeans_Eff {

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

        private Properties prop;
        private Job job;
        private IntWritable minIndexKey = new IntWritable(1);
        private Text textValue = new Text("");
        private PairOfDoublePointInt pDoublePointInt = new PairOfDoublePointInt();
        private HashMap<Integer, DoublePoint> centersMap;
        private List<DoublePoint> centers = new ArrayList();
        private DoublePoint point = new DoublePoint();
        private String outString;
        private String[] coords = new String[2];

        public Map() {
        }
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

            // Parse the input in the format x,y.
            StringTokenizer tok = new StringTokenizer(input.toString(), ",");
            int i = 0;
            while (tok.hasMoreTokens()) {
                coords[i++] = tok.nextToken();
            }

            //DoublePoint point = new DoublePoint();

            //coords = input.toString().split(",");
            point.setXY(Double.parseDouble(coords[0].trim()), Double.parseDouble(coords[1].trim()));

            double minDist = Double.MAX_VALUE;
            int minIndex = 0;
            int index = 0;
            double distance;
            // Assign the input to a cluster.
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

        @Override
        public void close() {
            if (job.getVariable("centersMap") != null) {
                job.setVariable("centersMap", null);
                job.setVariable("centers", null);
            }
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
        private final static Text TextOutput = new Text("");
        private final static DoublePoint doublePointOutput = new DoublePoint();
        private final static IntWritable empty = new IntWritable(-1);
        private HashMap<Integer, DoublePoint> centersMap = new HashMap();
        private HashMap<Integer, DoublePoint> oldCentersMap;
        private Job job;
        private String[] partialSums = new String[3];
        //private String outStr;

        /**
         * Get the centers from the configuration.
         */
        @Override
        public void setup(Reducer<IntWritable, PairOfDoublePointInt, IntWritable, DoublePoint>.Context context) {

            job = context.getJobObject();
            centersMap = (HashMap<Integer, DoublePoint>) job.getVariable("centersMap");
//            job.setVariable("centersMap", null);
//            job.setVariable("centers", null);

            //super.configure(conf);
//            if (centers == null) {
//                centers = new ArrayList<DoublePoint>();
//                String[] coords;
//                DoublePoint point;
//                Job job = context.getJobObject();
//
//                // Load the centers from the conf.
//                int count = Integer.parseInt((String) job.getVariable(KEY_PREFIX + "count"));
//
//                for (int i = 0; i < count; i++) {
//                    coords = ((String) job.getVariable(KEY_PREFIX + i)).split(",");
//                    point = new DoublePoint(Double.parseDouble(coords[0]), Double.parseDouble(coords[1]));
//                    centers.add(point);
//                }
//            }

            //centers.clear();
        }

        /**
         * Calculate the new averages for the clusters.
         */
        @Override
        public void reduce(IntWritable key, Iterable<PairOfDoublePointInt> values, final Context context) throws IOException, InterruptedException {
            // We receive the sums from the combiner, so we have to sum up those values and count.
            double x = 0;
            double y = 0;
            double count = 0;
            int ind = 0;
           // StringBuilder outStr = new StringBuilder();

            //String[] partialSums;
            PairOfDoublePointInt pDoublePointInt;

            for (Iterator<PairOfDoublePointInt> i = values.iterator(); i.hasNext();) {
                //partialSums = i.next().toString().split(",");
                pDoublePointInt = i.next();
//                StringTokenizer tok = new StringTokenizer(i.next().toString(), ",");
//                ind = 0;
//                while (tok.hasMoreTokens()) {
//                    partialSums[ind++] = tok.nextToken();
//                }

                x += pDoublePointInt.getKey().getX();
                y += pDoublePointInt.getKey().getY();
                count += pDoublePointInt.getValue();
//                x += Double.parseDouble(partialSums[0]);
//                y += Double.parseDouble(partialSums[1]);
//                count += Integer.parseInt(partialSums[2]);
            }

            // Calculate the averages to find the new cluster value.
            x /= count;
            y /= count;
            //outStr = outStr.append(Double.toString(x)).append(",").append(Double.toString(y));
            DoublePoint doublePoint = new DoublePoint(x, y);

            //centers.add(new DoublePoint(x, y));
            //centersMap.put(key.get(), new DoublePoint(x, y));
            job.setVariable(key.get() + "", doublePoint);
            System.out.println(key.get() + " " + x + " ********* " + y);

            // Check if we need to update our center.
            //TextOutput.set(outStr.toString());
            context.write(empty, doublePoint);
        }

        @Override
        public void close() {
            //job.setVariable("centersMap", centersMap);
            //System.gc();
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
        //job.setVariable("centersMap", centersMap);

        int ind = 0;
        for (DoublePoint dp : centers) {
            job.setVariable(ind + "", dp);
            ind++;
        }
        //job.setVariable("centers", centers);

        long start = System.currentTimeMillis();
        //JobClient.runJob(conf);
        job.waitForCompletion(true);
        long end = System.currentTimeMillis();

        for (int i = 0; i < 10000; ++i) {
            if (job.getVariable(i + "") != null) {
                centersMap.put(i, (DoublePoint) job.getVariable(i + ""));
            } else {
                break;
            }
        }
        //System.exit(0);

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
        for (String path : dataPaths) {
            //jobName = "kmeans-" + path.split("\\.")[0];
            jobName = "kmeans-" + path;
            fullDataPath = dataPath + "/" + path;
            fullClusterPath = dataPath + "/centers/centers-10.txt";
            outputPath = "/user/kmeans/output/" + jobName;

            String[] coords;
            DoublePoint point;
            List<DoublePoint> centers = new ArrayList();

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

            for (int i = 0; i < 10; ++i) {
                System.out.println("************************ Iteration: " + i + " ****************************");
                centersMap = (HashMap<Integer, DoublePoint>) this.createJob(jobName, fullDataPath, fullClusterPath, outputPath, 10, centersMap, centers);
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
        }
//        // Create jobs varying clusters.
//        for (String path : clusterPaths) {
//            //jobName = "kmeans-" + path.split(".")[0];
//            jobName = "kmeans-" + path;            
//            fullDataPath = dataPath + "/data-100m.txt";
//            fullClusterPath = dataPath + "/" + path;
//            outputPath = "/user/kmeans/output/" + jobName;
//            this.createJob(jobName, fullDataPath, fullClusterPath, outputPath, 10);
//        }
//
//        // Create jobs varying mappers.
//        for (int i = 5; i <= 20; i += 5) {
//            jobName = "kmeans-n-" + i;
//            fullDataPath = dataPath + "/data-100m.txt";
//            fullClusterPath = dataPath + "/centers-1000.txt";
//            outputPath = "/user/kmeans/output/" + jobName;
//            this.createJob(jobName, fullDataPath, fullClusterPath, outputPath, i);
//        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        //int res = ToolRunner.run(new Configuration(), new MapReduceKMeans(), args);
        MapReduceKMeans_Eff mapredKmeans = new MapReduceKMeans_Eff();
        mapredKmeans.run(args);
        //System.exit(res);

    }
}