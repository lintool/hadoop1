package test.hone.mrlda;

import com.google.common.base.Preconditions;
import edu.umd.cloud9.math.Gamma;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.PairOfInts;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceMemoryOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.DeleteFile;

public class VariationalInference_Tibanna {
    
    public static final double DEFAULT_ETA = Math.log(1e-8);
    public static final float DEFAULT_ALPHA_UPDATE_CONVERGE_THRESHOLD = 0.000001f;
    public static final int DEFAULT_ALPHA_UPDATE_MAXIMUM_ITERATION = 1000;
    public static final int DEFAULT_ALPHA_UPDATE_MAXIMUM_DECAY = 10;
    public static final float DEFAULT_ALPHA_UPDATE_DECAY_FACTOR = 0.8f;
    /**
     * @deprecated
     */
    public static final int DEFAULT_ALPHA_UPDATE_SCALE_FACTOR = 10;
    /**
     * @deprecated
     */
    public static final float DEFAULT_ALPHA_UPDATE_INITIAL = 100f;
    // specific settings
    public static final String TRUNCATE_BETA_OPTION = "truncatebeta";
    public static final String MAPPER_COMBINER_OPTION = "mappercombiner";
    // set the minimum memory threshold, in bytes
    public static final int MEMORY_THRESHOLD = 64 * 1024 * 1024;
    public static AtomicInteger counter_total_doc;
    public static AtomicInteger counter_total_term;
    public static double counter_log_likelihood;
    public static AtomicInteger counter_config_time;
    public static AtomicInteger counter_training_time;
    public static AtomicInteger counter_dummy_counter;
    private List<Future<Object>> reduceOutput;

    //static final Logger sLogger = Logger.getLogger(VariationalInference.class);
    static enum ParameterCounter {
        
        TOTAL_DOC, TOTAL_TERM, LOG_LIKELIHOOD, CONFIG_TIME, TRAINING_TIME, DUMMY_COUNTER,
    }
    
    @SuppressWarnings("unchecked")
    public int run(String[] args) throws Exception {
        
        counter_total_doc = new AtomicInteger();
        counter_total_term = new AtomicInteger();
        counter_config_time = new AtomicInteger();
        counter_training_time = new AtomicInteger();
        counter_dummy_counter = new AtomicInteger();
        
        boolean mapperCombiner = false;
        boolean truncateBeta = false;
        
        String inputPath = args[0];
        String outputPath = "data/mrlda/output";

        //boolean localMerge = FileMerger.LOCAL_MERGE;
        //boolean localMerge = false;
        boolean randomStartGamma = Settings.RANDOM_START_GAMMA;
        
        int numberOfTopics = 5;// Settings.DEFAULT_NUMBER_OF_TOPICS;
        int numberOfIterations = Settings.DEFAULT_GLOBAL_MAXIMUM_ITERATION;
        int mapperTasks = Settings.DEFAULT_NUMBER_OF_MAPPERS;
        //int reducerTasks = Settings.DEFAULT_NUMBER_OF_REDUCERS;
        int reducerTasks = Integer.parseInt(args[1]);        
        int numberOfTerms = 10;
        
        boolean resume = Settings.RESUME;
        String modelPath = null;
        int snapshotIndex = 0;
        boolean training = Settings.LEARNING_MODE;
        
        Path informedPrior = null;
        
        return run(inputPath, outputPath, numberOfTopics, numberOfTerms, numberOfIterations,
                mapperTasks, reducerTasks, training, randomStartGamma, resume, informedPrior,
                modelPath, snapshotIndex, mapperCombiner, truncateBeta);
    }
    
    private int run(String inputPath, String outputPath, int numberOfTopics, int numberOfTerms,
            int numberOfIterations, int mapperTasks, int reducerTasks,
            boolean training, boolean randomStartGamma, boolean resume, Path informedPrior,
            String modelPath, int snapshotIndex, boolean mapperCombiner, boolean truncateBeta)
            throws Exception {
        
        Configuration conf = new Configuration();
        //JobConf conf = new JobConf(VariationalInference.class);
        Job job = new Job(conf);
        // FileSystem fs = FileSystem.get(conf);

        // delete the overall output path
        Path outputDir = new Path(outputPath);
        if (!resume && (new File(outputDir.toString())).exists()) {
            //fs.delete(outputDir, true);
            //DeleteFile.delete(new File(outputDir.toString()));
            //fs.mkdirs(outputDir);
            //(new File(outputDir.toString())).mkdirs();
        }
        
        if (informedPrior != null) {
            Path eta = informedPrior;
            Preconditions.checkArgument((new File(informedPrior.toString()).exists()) && (new File(informedPrior.toString()).isFile()), "Illegal informed prior file...");
            informedPrior = new Path(outputPath + InformedPrior.ETA);
            //FileUtil.copy(fs, eta, fs, informedPrior, false, conf);
        }
        
        Path inputDir = new Path(inputPath);
        Path tempDir = new Path(outputPath + Settings.TEMP);
        
        Path alphaDir = null;
        Path betaDir = null;
        
        Path documentGlobDir = new Path(tempDir.toString() + Path.SEPARATOR + Settings.GAMMA + Settings.STAR);

        // these parameters are NOT used at all in the case of testing mode
        Path alphaSufficientStatisticsDir = new Path(tempDir.toString() + Path.SEPARATOR + "part-00000");
        String betaGlobDir = tempDir.toString() + Path.SEPARATOR + Settings.BETA + Settings.STAR;
        
        SequenceFile.Reader sequenceFileReader = null;
        SequenceFile.Writer sequenceFileWriter = null;
        
        String betaPath = outputPath + Settings.BETA;
        String alphaPath = outputPath + Settings.ALPHA;
        double[] alphaVector = new double[numberOfTopics];
        
        if (!training) {
            alphaDir = new Path(modelPath + Settings.BETA + snapshotIndex);
            betaDir = new Path(modelPath + Settings.ALPHA + snapshotIndex);
        } else {
            if (!resume) {
                // initialize alpha vector randomly - if it doesn't already exist
                alphaDir = new Path(alphaPath + 0);
                for (int i = 0; i < alphaVector.length; i++) {
                    alphaVector[i] = Math.random();
                }
                try {
                    //sequenceFileWriter = new SequenceFile.Writer(conf, alphaDir, IntWritable.class, DoubleWritable.class);
                    //exportAlpha(sequenceFileWriter, alphaVector);
                } finally {
                    //IOUtils.closeStream(sequenceFileWriter);
                    //sequenceFileWriter.close();
                }
            } else {
                alphaDir = new Path(alphaPath + snapshotIndex);
                betaDir = new Path(betaPath + snapshotIndex);
                
                inputDir = new Path(outputPath + Settings.GAMMA + snapshotIndex);
            }
        }
        
        double lastLogLikelihood = 0;
        int iterationCount = snapshotIndex;
        int numberOfDocuments = 5;
        
        do {
            //conf = new JobConf(VariationalInference.class);
            //job = new Job(conf);
            job.setVariable("alphaVector", alphaVector);
            if (training) {
                //conf.setJobName(VariationalInference.class.getSimpleName() + " - Iteration "                        + (iterationCount + 1));
            } else {
                // conf.setJobName(VariationalInference.class.getSimpleName() + " - Test");
            }
            //fs = FileSystem.get(conf);

            if (iterationCount != 0) {
                //Preconditions.checkArgument((new File(betaDir.toString())).exists(), "Missing model parameter beta...");
                //DistributedCache.addCacheFile(betaDir.toUri(), conf);
            }

            //Preconditions.checkArgument((new File(alphaDir.toString())).exists(), "Missing model parameter alpha...");
            // DistributedCache.addCacheFile(alphaDir.toUri(), conf);

            if (informedPrior != null) {
                //Preconditions.checkArgument((new File(informedPrior.toString())).exists(), "Informed prior does not exist...");
                //DistributedCache.addCacheFile(informedPrior.toUri(), conf);
            }

            //conf.setFloat(Settings.PROPERTY_PREFIX + "model.mapper.converge.gamma", Settings.DEFAULT_GAMMA_UPDATE_CONVERGE_THRESHOLD);
            //conf.setFloat(Settings.PROPERTY_PREFIX + "model.mapper.converge.likelihood", Settings.DEFAULT_GAMMA_UPDATE_CONVERGE_CRITERIA);
            conf.setInt(Settings.PROPERTY_PREFIX + "model.mapper.converge.iteration", Settings.MAXIMUM_GAMMA_ITERATION);
            
            conf.setInt(Settings.PROPERTY_PREFIX + "model.topics", numberOfTopics);
            conf.setInt(Settings.PROPERTY_PREFIX + "corpus.terms", numberOfTerms);
            conf.setBoolean(Settings.PROPERTY_PREFIX + "model.train", training);
            conf.setBoolean(Settings.PROPERTY_PREFIX + "model.random.start", randomStartGamma);
            conf.setBoolean(Settings.PROPERTY_PREFIX + "model.informed.prior", informedPrior != null);
            conf.setBoolean(Settings.PROPERTY_PREFIX + "model.mapper.combiner", mapperCombiner);
            conf.setBoolean(Settings.PROPERTY_PREFIX + "model.truncate.beta", truncateBeta && iterationCount >= 5);

            // conf.setInt("mapred.task.timeout", VariationalInference.DEFAULT_MAPRED_TASK_TIMEOUT);
            // conf.set("mapred.child.java.opts", "-Xmx2048m");

            job.setNumMapTasks(mapperTasks);
            job.setNumReduceTasks(reducerTasks);
            
            if (training) {
                //MultipleOutputs.addMultiNamedOutput(conf, Settings.BETA, SequenceFileOutputFormat.class, PairOfIntFloat.class, HMapIFW.class);
            }
            
            if (!randomStartGamma || !training) {
                //MultipleOutputs.addMultiNamedOutput(conf, Settings.GAMMA, SequenceFileOutputFormat.class, IntWritable.class, Document.class);
            }
            
            job.setIterationNo(iterationCount);
            
            job.setFirstIterationInputFromDisk(true);
            job.setInputDirectoryPath(inputDir.toString());
            job.setInMemoryInput(true);
            
            job.setMapperClass(DocumentMapper.class);
            job.setReducerClass(TermReducer.class);
            //job.setCombinerClass(TermCombiner.class);
            job.setPartitionerClass(HashPartitioner.class);
            
            job.setMapOutputKeyClass(PairOfInts.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            //job.setCompressMapOutput(false);
            //FileOutputFormat.setCompressOutput(conf, true);

            //FileInputFormat.setInputPaths(conf, inputDir);
            //FileOutputFormat.setOutputPath(conf, tempDir);
            job.setOutputFile(new File(tempDir.toString()));

            // suppress the empty part files
            job.setInputFormatClass(SequenceIntDocumentInputFormat.class);
            job.setOutputFormatClass(SequenceMemoryOutputFormat.class);

            // delete the output directory if it exists already
            //fs.delete(tempDir, true);
            //DeleteFile.delete(new File(tempDir.toString()));

            long startTime = System.currentTimeMillis();
            //Configuration conf = new Configuration();
            //RunningJob job = JobClient.runJob(conf);
            job.waitForCompletion(true);
            job.setMapInputFromReducer(false);
            //sLogger.info("Iteration " + (iterationCount + 1) + " finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

            //Counters counters = job.getCounters();
            //double logLikelihood = -counters.findCounter(ParameterCounter.LOG_LIKELIHOOD).getCounter()                    * 1.0 / Settings.DEFAULT_COUNTER_SCALE;
            //sLogger.info("Log likelihood of the model is: " + logLikelihood);

            double logLikelihood = -counter_log_likelihood;
            //numberOfDocuments = (int) counters.findCounter(ParameterCounter.TOTAL_DOC).getCounter();
            numberOfDocuments = counter_total_doc.get();

            //sLogger.info("Total number of documents is: " + numberOfDocuments);
            //numberOfTerms = (int) (counters.findCounter(ParameterCounter.TOTAL_TERM).getCounter() / numberOfTopics);
            numberOfTerms = counter_total_term.get();
            //sLogger.info("Total number of term is: " + numberOfTerms);

            //double configurationTime = counters.findCounter(ParameterCounter.CONFIG_TIME).getCounter()                    * 1.0 / numberOfDocuments;
            double configurationTime = counter_config_time.get();
            // sLogger.info("Average time elapsed for mapper configuration (ms): " + configurationTime);
            //double trainingTime = counters.findCounter(ParameterCounter.TRAINING_TIME).getCounter() * 1.0 / numberOfDocuments;
            double trainingTime = counter_training_time.get();
            //sLogger.info("Average time elapsed for processing a document (ms): " + trainingTime);

            // break out of the loop if in testing mode
            if (!training) {
                break;
            }

            // merge gamma (for alpha update) first and move document to the correct directory
            if (!randomStartGamma) {
                // TODO: resume got error
                if (iterationCount != 0) {
                    // remove old gamma and document output
                    //fs.delete(inputDir, true);
                    //DeleteFile.delete(new File(inputDir.toString()));
                }
                //inputDir = new Path(outputPath + Settings.GAMMA + (iterationCount + 1));

                //(new File(inputDir.toString())).mkdirs();
//                fs.mkdirs(inputDir);
//                FileStatus[] fileStatus = fs.globStatus(documentGlobDir);
//                for (FileStatus file : fileStatus) {
//                    Path newPath = new Path(inputDir.toString() + Path.SEPARATOR + file.getPath().getName());
//                    fs.rename(file.getPath(), newPath);
//                }
            }

            // update alpha's
            try {
                DataInputStream dis;
                reduceOutput = job.getReduceOutput();
                byte[] sequenceBytes;
                IntWritable intWritable = new IntWritable();
                DoubleWritable doubleWritable = new DoubleWritable();
                ArrayList<Double> doubleList = new ArrayList<Double>();
                ByteBuffer byteBuffer;
                for (Future< Object> fut : reduceOutput) {
                    byteBuffer = (ByteBuffer) fut.get();
                    byteBuffer.clear();
                    sequenceBytes = new byte[byteBuffer.remaining()];
                    byteBuffer.get(sequenceBytes, 0, sequenceBytes.length);
                    
                    dis = new DataInputStream(new BufferedInputStream(new ByteArrayInputStream(sequenceBytes)));
                    while (dis.available() > 0) {
                        intWritable.readFields(dis);
                        doubleWritable.readFields(dis);
                        doubleList.add(doubleWritable.get());
                    }
                }

//                for (double val : alphaVector) {
//                    System.out.println("alpha val before update: " + val);
//                }
//
//                for (Object obj : doubleList) {
//                    System.out.println("alpha val--- before update: " + ((Double) obj));
//                }

                //job.remVariable("alphaVector");
                // load old alpha's into the system
                double[] updatedAlphaVector = updateVectorAlpha(numberOfTopics, numberOfDocuments, alphaVector, doubleList.toArray());

//                for (double val : updatedAlphaVector) {
//                    System.out.println("alpha val after update: " + val);
//                }

                alphaVector = updatedAlphaVector;


                //sequenceFileReader = new SequenceFile.Reader(alphaDir, conf);
                //alphaVector = importAlpha(sequenceFileReader, numberOfTopics);
                //sLogger.info("Successfully import old alpha vector from file " + alphaDir);

                // load alpha sufficient statistics into the system
//                double[] alphaSufficientStatistics = null;
//                sequenceFileReader = new SequenceFile.Reader(alphaSufficientStatisticsDir, conf);
//                alphaSufficientStatistics = importAlpha(sequenceFileReader, numberOfTopics);
//                //sLogger.info("Successfully import alpha sufficient statistics tokens from file " + alphaSufficientStatisticsDir);
//
//                // update alpha
//                alphaVector = updateVectorAlpha(numberOfTopics, numberOfDocuments, alphaVector,
//                        alphaSufficientStatistics);
                //sLogger.info("Successfully update new alpha vector.");

                // output the new alpha's to the system
                //alphaDir = new Path(alphaPath + (iterationCount + 1));
                //sequenceFileWriter = new SequenceFile.Writer(conf, alphaDir, IntWritable.class, DoubleWritable.class);
                //exportAlpha(sequenceFileWriter, alphaVector);
                //sLogger.info("Successfully export new alpha vector to file " + alphaDir);

                // remove all the alpha sufficient statistics
                //fs.deleteOnExit(alphaSufficientStatisticsDir);
                //DeleteFile.delete(new File(alphaSufficientStatisticsDir.toString()));
//                sequenceFileReader.close();
                //sequenceFileWriter.close();
            } finally {
//                IOUtils.closeStream(sequenceFileReader);
                //sequenceFileReader.close();
                //sequenceFileWriter.close();
//                IOUtils.closeStream(sequenceFileWriter);
            }

            // merge beta's
            // TODO: local merge doesn't compress data
//            if (localMerge) {
//                betaDir = FileMerger.mergeSequenceFiles(betaGlobDir, betaPath + (iterationCount + 1), 0,
//                        PairOfIntFloat.class, HMapIFW.class, true, true);
//            } else {
//                betaDir = FileMerger.mergeSequenceFiles(betaGlobDir, betaPath + (iterationCount + 1),
//                        reducerTasks, PairOfIntFloat.class, HMapIFW.class, true, true);
//            }

            //sLogger.info("Log likelihood after iteration " + (iterationCount + 1) + " is " + logLikelihood);
            if (Math.abs((lastLogLikelihood - logLikelihood) / lastLogLikelihood) <= Settings.DEFAULT_GLOBAL_CONVERGE_CRITERIA) {
                //  sLogger.info("Model converged after " + (iterationCount + 1) + " iterations...");
                break;
            }
            lastLogLikelihood = logLikelihood;
            
            iterationCount++;
        } while (iterationCount < numberOfIterations);
        
        return 0;
    }
    
    public static double[] updateVectorAlpha(int numberOfTopics, int numberOfDocuments,
            double[] alphaVector, double[] alphaSufficientStatistics) {
        double[] alphaVectorUpdate = new double[numberOfTopics];
        double[] alphaGradientVector = new double[numberOfTopics];
        double[] alphaHessianVector = new double[numberOfTopics];
        
        int alphaUpdateIterationCount = 0;

        // update the alpha vector until converge
        boolean keepGoing = true;
        try {
            int decay = 0;
            
            double alphaSum = 0;
            for (int j = 0; j < numberOfTopics; j++) {
                alphaSum += alphaVector[j];
            }
            
            while (keepGoing) {
                double sumG_H = 0;
                double sum1_H = 0;
                
                for (int i = 0; i < numberOfTopics; i++) {
                    // compute alphaGradient
                    alphaGradientVector[i] = numberOfDocuments
                            * (Gamma.digamma(alphaSum) - Gamma.digamma(alphaVector[i]))
                            + alphaSufficientStatistics[i];

                    // compute alphaHessian
                    alphaHessianVector[i] = -numberOfDocuments * Gamma.trigamma(alphaVector[i]);
                    
                    if (alphaGradientVector[i] == Double.POSITIVE_INFINITY
                            || alphaGradientVector[i] == Double.NEGATIVE_INFINITY) {
                        throw new ArithmeticException("Invalid ALPHA gradient matrix...");
                    }
                    
                    sumG_H += alphaGradientVector[i] / alphaHessianVector[i];
                    sum1_H += 1 / alphaHessianVector[i];
                }
                
                double z = numberOfDocuments * Gamma.trigamma(alphaSum);
                double c = sumG_H / (1 / z + sum1_H);
                
                while (true) {
                    boolean singularHessian = false;
                    
                    for (int i = 0; i < numberOfTopics; i++) {
                        double stepSize = Math.pow(VariationalInference_Tibanna.DEFAULT_ALPHA_UPDATE_DECAY_FACTOR,
                                decay) * (alphaGradientVector[i] - c) / alphaHessianVector[i];
                        if (alphaVector[i] <= stepSize) {
                            // the current hessian matrix is singular
                            singularHessian = true;
                            break;
                        }
                        alphaVectorUpdate[i] = alphaVector[i] - stepSize;
                    }
                    
                    if (singularHessian) {
                        // we need to further reduce the step size
                        decay++;

                        // recover the old alpha vector
                        alphaVectorUpdate = alphaVector;
                        if (decay > VariationalInference_Tibanna.DEFAULT_ALPHA_UPDATE_MAXIMUM_DECAY) {
                            break;
                        }
                    } else {
                        // we have successfully update the alpha vector
                        break;
                    }
                }

                // compute the alpha sum and check for alpha converge
                alphaSum = 0;
                keepGoing = false;
                for (int j = 0; j < numberOfTopics; j++) {
                    alphaSum += alphaVectorUpdate[j];
                    if (Math.abs((alphaVectorUpdate[j] - alphaVector[j]) / alphaVector[j]) >= VariationalInference_Tibanna.DEFAULT_ALPHA_UPDATE_CONVERGE_THRESHOLD) {
                        keepGoing = true;
                    }
                }
                
                if (alphaUpdateIterationCount >= VariationalInference_Tibanna.DEFAULT_ALPHA_UPDATE_MAXIMUM_ITERATION) {
                    keepGoing = false;
                }
                
                if (decay > VariationalInference_Tibanna.DEFAULT_ALPHA_UPDATE_MAXIMUM_DECAY) {
                    break;
                }
                
                alphaUpdateIterationCount++;
                alphaVector = alphaVectorUpdate;
            }
        } catch (IllegalArgumentException iae) {
            System.err.println(iae.getMessage());
            iae.printStackTrace();
        } catch (ArithmeticException ae) {
            System.err.println(ae.getMessage());
            ae.printStackTrace();
        }
        
        return alphaVector;
    }
    
    public static double[] updateVectorAlpha(int numberOfTopics, int numberOfDocuments, double[] alphaVector, Object[] alphaSufficientStatistics) {
        double[] alphaVectorUpdate = new double[numberOfTopics];
        double[] alphaGradientVector = new double[numberOfTopics];
        double[] alphaHessianVector = new double[numberOfTopics];
        
        int alphaUpdateIterationCount = 0;

        // update the alpha vector until converge
        boolean keepGoing = true;
        try {
            int decay = 0;
            
            double alphaSum = 0;
            for (int j = 0; j < numberOfTopics; j++) {
                alphaSum += alphaVector[j];
            }
            
            while (keepGoing) {
                double sumG_H = 0;
                double sum1_H = 0;
                
                for (int i = 0; i < numberOfTopics; i++) {
                    // compute alphaGradient
                    alphaGradientVector[i] = numberOfDocuments * (Gamma.digamma(alphaSum) - Gamma.digamma(alphaVector[i])) + ((Double) alphaSufficientStatistics[i]);

                    //System.out.println("Alpha value: " + alphaVector[i] + " Gamma: " + Gamma.trigamma(alphaVector[i]));
                    // compute alphaHessian
                    alphaHessianVector[i] = -numberOfDocuments * Gamma.trigamma(alphaVector[i]);
                    
                    if (alphaGradientVector[i] == Double.POSITIVE_INFINITY
                            || alphaGradientVector[i] == Double.NEGATIVE_INFINITY) {
                        throw new ArithmeticException("Invalid ALPHA gradient matrix...");
                    }

                    //System.out.println("gradVect: " + alphaGradientVector[i] + " hessVect: " + alphaHessianVector[i]+" noDoc: "+numberOfDocuments);

                    sumG_H += alphaGradientVector[i] / alphaHessianVector[i];
                    sum1_H += 1 / alphaHessianVector[i];
                }
                
                double z = numberOfDocuments * Gamma.trigamma(alphaSum);
                double c = sumG_H / (1 / z + sum1_H);
                //System.out.println("Culprit: numDoc: " + numberOfDocuments + " alphaSum: " + alphaSum + " Gamma: " + Gamma.trigamma(alphaSum));

                
                while (true) {
                    boolean singularHessian = false;
                    
                    for (int i = 0; i < numberOfTopics; i++) {
                        double stepSize = Math.pow(VariationalInference_Tibanna.DEFAULT_ALPHA_UPDATE_DECAY_FACTOR,
                                decay) * (alphaGradientVector[i] - c) / alphaHessianVector[i];
                        // System.out.println("Culprit: pow: " + Math.pow(VariationalInference_Tibanna.DEFAULT_ALPHA_UPDATE_DECAY_FACTOR,                                decay) + " alphaGrad: " + alphaGradientVector[i] + " c: " + c + " alphahess: " + alphaHessianVector[i]);

                        if (alphaVector[i] <= stepSize) {
                            // the current hessian matrix is singular
                            singularHessian = true;
                            break;
                        }
                        alphaVectorUpdate[i] = alphaVector[i] - stepSize;
                        //System.out.println("alpha vector for calc: " + alphaVector[i] + " Step size: " + stepSize);
                        //System.out.println("alpha vector update: " + alphaVectorUpdate[i]);
                    }
                    
                    if (singularHessian) {
                        // we need to further reduce the step size
                        decay++;

                        // recover the old alpha vector
                        alphaVectorUpdate = alphaVector;
                        if (decay > VariationalInference_Tibanna.DEFAULT_ALPHA_UPDATE_MAXIMUM_DECAY) {
                            break;
                        }
                    } else {
                        // we have successfully update the alpha vector
                        break;
                    }
                }

                // compute the alpha sum and check for alpha converge
                alphaSum = 0;
                keepGoing = false;
                for (int j = 0; j < numberOfTopics; j++) {
                    alphaSum += alphaVectorUpdate[j];
                    if (Math.abs((alphaVectorUpdate[j] - alphaVector[j]) / alphaVector[j]) >= VariationalInference_Tibanna.DEFAULT_ALPHA_UPDATE_CONVERGE_THRESHOLD) {
                        keepGoing = true;
                    }
                }
                
                if (alphaUpdateIterationCount >= VariationalInference_Tibanna.DEFAULT_ALPHA_UPDATE_MAXIMUM_ITERATION) {
                    keepGoing = false;
                }
                
                if (decay > VariationalInference_Tibanna.DEFAULT_ALPHA_UPDATE_MAXIMUM_DECAY) {
                    break;
                }
                
                alphaUpdateIterationCount++;
                alphaVector = alphaVectorUpdate;
            }
        } catch (IllegalArgumentException iae) {
            System.err.println(iae.getMessage());
            iae.printStackTrace();
        } catch (ArithmeticException ae) {
            System.err.println(ae.getMessage());
            ae.printStackTrace();
        }

//        for (double val : alphaVector) {
//            System.out.println("Return alpha ****: " + val);
//        }

        
        return alphaVector;
    }

    /**
     * @deprecated @param numberOfTopics
     * @param numberOfDocuments
     * @param alphaInit
     * @param alphaSufficientStatistics
     * @return
     */
    public static double updateScalarAlpha(int numberOfTopics, int numberOfDocuments,
            double alphaInit, double alphaSufficientStatistics) {
        int alphaUpdateIterationCount = 0;
        double alphaGradient = 0;
        double alphaHessian = 0;

        // update the alpha vector until converge
        boolean keepGoing = true;
        double alphaUpdate = alphaInit;
        try {
            double alphaSum = alphaUpdate * numberOfTopics;
            
            while (keepGoing) {
                alphaUpdateIterationCount++;
                
                if (Double.isNaN(alphaUpdate) || Double.isInfinite(alphaUpdate)) {
                    alphaInit *= VariationalInference_Tibanna.DEFAULT_ALPHA_UPDATE_SCALE_FACTOR;
                    alphaUpdate = alphaInit;
                }
                
                alphaSum = alphaUpdate * numberOfTopics;

                // compute alphaGradient
                alphaGradient = numberOfDocuments
                        * (numberOfTopics * Gamma.digamma(alphaSum) - numberOfTopics
                        * Gamma.digamma(alphaUpdate)) + alphaSufficientStatistics;

                // compute alphaHessian
                alphaHessian = numberOfDocuments
                        * (numberOfTopics * numberOfTopics * Gamma.trigamma(alphaSum) - numberOfTopics
                        * Gamma.trigamma(alphaUpdate));
                
                alphaUpdate = Math.exp(Math.log(alphaUpdate) - alphaGradient
                        / (alphaHessian * alphaUpdate + alphaGradient));
                
                if (Math.abs(alphaGradient) < VariationalInference_Tibanna.DEFAULT_ALPHA_UPDATE_CONVERGE_THRESHOLD) {
                    break;
                }
                
                if (alphaUpdateIterationCount > VariationalInference_Tibanna.DEFAULT_ALPHA_UPDATE_MAXIMUM_ITERATION) {
                    break;
                }
            }
        } catch (IllegalArgumentException iae) {
            System.err.println(iae.getMessage());
            iae.printStackTrace();
        } catch (ArithmeticException ae) {
            System.err.println(ae.getMessage());
            ae.printStackTrace();
        }
        
        return alphaUpdate;
    }
    
    public static double[] importAlpha(SequenceFile.Reader sequenceFileReader, int numberOfTopics)
            throws IOException {
        
        double[] alpha = new double[numberOfTopics];
        int counts = 0;
        
        IntWritable intWritable = new IntWritable();
        DoubleWritable doubleWritable = new DoubleWritable();
        
        while (sequenceFileReader.next(intWritable, doubleWritable)) {
            Preconditions.checkArgument(intWritable.get() > 0 && intWritable.get() <= numberOfTopics,
                    "Invalid alpha index: " + intWritable.get() + "...");

            // topic is from 1 to K
            alpha[intWritable.get() - 1] = doubleWritable.get();
            counts++;
        }
        
        Preconditions.checkArgument(counts == numberOfTopics, "Invalid alpha vector...");
        
        return alpha;
    }
    
    public static void exportAlpha(SequenceFile.Writer sequenceFileWriter, double[] alpha)
            throws IOException {
        IntWritable intWritable = new IntWritable();
        DoubleWritable doubleWritable = new DoubleWritable();
        for (int i = 0; i < alpha.length; i++) {
            doubleWritable.set(alpha[i]);
            intWritable.set(i + 1);
            sequenceFileWriter.append(intWritable, doubleWritable);
        }
        //sequenceFileWriter.close();
    }
    
    public static void main(String[] args) throws Exception {
        //int res = ToolRunner.run(new Configuration(), new VariationalInference(), args);
        if (args.length != 2) {
            System.out.println("Total entered args: " + args.length);
            System.out.println("Args: InputDir numReducers");
            System.exit(-1);
        }
        VariationalInference_Tibanna varInf = new VariationalInference_Tibanna();
        varInf.run(args);
        //System.exit(res);
    }
}
