package test.hone.mrlda;

//import cc.mrlda.VariationalInference_Tibanna.ParameterCounter;
import com.google.common.base.Preconditions;
import edu.umd.cloud9.math.LogMath;
import edu.umd.cloud9.util.map.HMapIV;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.map.HMapIFW;

public class TermReducer extends Reducer<PairOfInts, DoubleWritable, IntWritable, DoubleWritable> {

    boolean truncateBeta = false;
    // double truncationThreshold = Math.log(0.001);
    int truncationSize = 10000;
    TreeMap<Double, Integer> treeMap = new TreeMap<Double, Integer>();
    Iterator<Entry<Double, Integer>> itr = null;
    private HMapIV<Set<Integer>> lambdaMap = null;
    private static boolean learning = Settings.LEARNING_MODE;
    private int topicIndex = 0;
    private double normalizeFactor = 0;
    private MultipleOutputs multipleOutputs;
    private OutputCollector<PairOfIntFloat, HMapIFW> outputBeta;
    private IntWritable intWritable = new IntWritable();
    private DoubleWritable doubleWritable = new DoubleWritable();
    private PairOfIntFloat outputKey = new PairOfIntFloat();
    private HMapIFW outputValue = new HMapIFW();
    private DataOutputStream outputBetaStream;
    private ByteArrayOutputStream_test baosBeta;
    private Job job;

    @Override
    public void setup(Reducer<PairOfInts, DoubleWritable, IntWritable, DoubleWritable>.Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        multipleOutputs = new MultipleOutputs(conf);
        learning = conf.getBoolean(Settings.PROPERTY_PREFIX + "model.train", Settings.LEARNING_MODE);
        truncateBeta = conf.getBoolean(Settings.PROPERTY_PREFIX + "model.truncate.beta", false);
        boolean informedPrior = conf.getBoolean(Settings.PROPERTY_PREFIX + "model.informed.prior", false);
        Path[] inputFiles;
        SequenceFile.Reader sequenceFileReader = null;
        baosBeta = new ByteArrayOutputStream_test();
        outputBetaStream = new DataOutputStream(new BufferedOutputStream(baosBeta));
        this.job = context.getJobObject();


//    try {
//      inputFiles = DistributedCache.getLocalCacheFiles(conf);
//
//      if (inputFiles != null) {
//        for (Path path : inputFiles) {
//          try {
//            sequenceFileReader = new SequenceFile.Reader(FileSystem.getLocal(conf), path, conf);
//
//            if (path.getName().startsWith(Settings.ALPHA)) {
//              continue;
//            } else if (path.getName().startsWith(Settings.BETA)) {
//              continue;
//            } else if (path.getName().startsWith(InformedPrior.ETA)) {
//              Preconditions.checkArgument(lambdaMap == null,
//                  "Lambda matrix was initialized already...");
//              lambdaMap = InformedPrior.importEta(sequenceFileReader);
//            } else {
//              throw new IllegalArgumentException("Unexpected file in distributed cache: "
//                  + path.getName());
//            }
//          } catch (IllegalArgumentException iae) {
//            iae.printStackTrace();
//          } catch (IOException ioe) {
//            ioe.printStackTrace();
//          } finally {
//            IOUtils.closeStream(sequenceFileReader);
//          }
//        }
//      }
//    } catch (IOException ioe) {
//      ioe.printStackTrace();
//    }

        Preconditions.checkArgument(informedPrior == (lambdaMap != null),
                "Fail to initialize informed prior...");

        // System.out.println("======================================================================");
        // System.out.println("Available processors (cores): "
        // + Runtime.getRuntime().availableProcessors());
        // long maxMemory = Runtime.getRuntime().maxMemory();
        // System.out.println("Maximum memory (bytes): "
        // + (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory));
        // System.out.println("Free memory (bytes): " + Runtime.getRuntime().freeMemory());
        // System.out.println("Total memory (bytes): " + Runtime.getRuntime().totalMemory());
        // System.out.println("======================================================================");
    }

    @Override
    public void reduce(PairOfInts key, Iterable<DoubleWritable> iterable, Context context) throws IOException, InterruptedException {
        Iterator<DoubleWritable> values = iterable.iterator();

        if (key.getLeftElement() == 0) {
            double sum = values.next().get();
            while (values.hasNext()) {
                sum += values.next().get();
            }

            Preconditions.checkArgument(key.getRightElement() > 0, "Unexpected sequence order for alpha sufficient statistics: " + key.toString());
        
            intWritable.set(key.getRightElement());
            doubleWritable.set(sum);
            context.write(intWritable, doubleWritable);

            //System.out.println("Reducer Yes output");
            return;
        }

        //System.out.println("Reducer No output");
        // I would be very surprised to get here...
        Preconditions.checkArgument(learning, "Invalid key from Mapper");
        //reporter.incrCounter(ParameterCounter.TOTAL_TERM, 1);
        VariationalInference_Tibanna.counter_total_term.incrementAndGet();

        double phiValue = values.next().get();
        while (values.hasNext()) {
            phiValue = LogMath.add(phiValue, values.next().get());
        }

        if (lambdaMap != null) {
            phiValue = LogMath.add(
                    InformedPrior.getEta(key.getRightElement(), lambdaMap.get(topicIndex)), phiValue);
        }

        if (topicIndex != key.getLeftElement()) {
            if (topicIndex == 0) {
                //outputBeta = multipleOutputs.getCollector(Settings.BETA, Settings.BETA);
            } else {
                outputKey.set(topicIndex, (float) normalizeFactor);

                if (truncateBeta) {
                    itr = treeMap.entrySet().iterator();
                    Entry<Double, Integer> temp = null;
                    outputValue.clear();
                    while (itr.hasNext()) {
                        temp = itr.next();
                        outputValue.put(temp.getValue(), temp.getKey().floatValue());
                    }
                }

                //outputBeta.collect(outputKey, outputValue);
                outputKey.write(outputBetaStream);
                outputValue.write(outputBetaStream);
            }

            topicIndex = key.getLeftElement();
            normalizeFactor = phiValue;
            if (truncateBeta) {
                treeMap.clear();
                treeMap.put(phiValue, key.getRightElement());
            } else {
                outputValue.clear();
                outputValue.put(key.getRightElement(), (float) phiValue);
            }
        } else {
            if (truncateBeta) {
                if (treeMap.size() >= truncationSize) {
                    if (treeMap.firstKey() < phiValue) {
                        normalizeFactor = Math.log(Math.exp(normalizeFactor) - Math.exp(treeMap.firstKey()));
                        treeMap.remove(treeMap.firstKey());

                        treeMap.put(phiValue, key.getRightElement());
                        normalizeFactor = LogMath.add(normalizeFactor, phiValue);
                    }
                } else {
                    treeMap.put(phiValue, key.getRightElement());
                    normalizeFactor = LogMath.add(normalizeFactor, phiValue);
                }
            } else {
                normalizeFactor = LogMath.add(normalizeFactor, phiValue);
                outputValue.put(key.getRightElement(), (float) phiValue);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (truncateBeta) {
            if (!treeMap.isEmpty()) {
                outputKey.set(topicIndex, (float) normalizeFactor);
                itr = treeMap.entrySet().iterator();
                Entry<Double, Integer> temp = null;
                outputValue.clear();
                while (itr.hasNext()) {
                    temp = itr.next();
                    outputValue.put(temp.getValue(), temp.getKey().floatValue());
                }
                //outputBeta.collect(outputKey, outputValue);
                outputKey.write(outputBetaStream);
                outputValue.write(outputBetaStream);
            }
        } else {
            if (!outputValue.isEmpty()) {
                outputKey.set(topicIndex, (float) normalizeFactor);
                //outputBeta.collect(outputKey, outputValue);
                outputKey.write(outputBetaStream);
                outputValue.write(outputBetaStream);
                outputBetaStream.flush();
                outputBetaStream.close();
                baosBeta.flush();
                job.setVariable("reduce-beta " + outputKey, baosBeta.toByteArray());
                baosBeta.close();
            }
        }
        //multipleOutputs.close();
    }
}