/**
 *
 */
package org.apache.hadoop.mapreduce;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.InputFormat;
import org.apache.hadoop.mapreduce.lib.output.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.RecordReader;

/**
 * The job to run
 *
 * @author tim
 */
public class Job {

    private String name;
    private final Configuration config;
    private int numMapperWorkers = -1;
    private int numReducerWorkers = -1;
    private Class<? extends Mapper> mapperClass;
    private Class<? extends Combiner> combinerClass;
    private Class<? extends Reducer> reducerClass;
    private Class<? extends Partitioner> partitionerClass;
    private Class<? extends InputFormat> inputFormat;
    private Class<? extends OutputFormat> outputFormat;
    private Class<? extends WritableComparable> mapKeyOutputClass;
    private Class<? extends WritableComparable> mapOutputValueClass;
    private Class<? extends WritableComparable> outputKeyClass;
    private Class<? extends WritableComparable> outputValueClass;
    private Class<? extends WritableComparable> mapKeyInputClass;
    private Class<? extends WritableComparable> mapValueInputClass;
    private Class<? extends RecordReader> reducerRecordReaderClass;
    private String inputDirectoryPath;
    private boolean inMemoryInput = false;
    private boolean inMemoryOutput = false;
    private boolean firstIterationInputFromDisk;
    private boolean iterationOutputToDisk;
    private int endIter;
    private int iterationNo = 0;
    private List<Future<Object>> reduceOutput;
    private ConcurrentMap<String, Object> varMap;
    private boolean mapInputFromReducer = false;
    // the main job input files
    protected List<File> inputFiles = new ArrayList<File>();
    // the job output file
    protected File outputFile;

    /**
     * @param config for the job
     * @param name Used in logging
     */
    public Job(Configuration config, String name) {
        this.name = name;
        this.config = config;
        this.varMap = new ConcurrentHashMap<String, Object>();
    }

    public Job(Configuration config) {
        this.config = config;
        this.name = "";
        this.varMap = new ConcurrentHashMap<String, Object>();
    }

    /**
     * @param blockUntilFinished should this run in the same thread or submit
     * and return?
     * @return true if no errors (always)
     */
    public final boolean waitForCompletion(final boolean blockUntilFinished) {
        final JobExecutor executor = new JobExecutor();
        executor.execute(this, blockUntilFinished);
        return true;
    }

    public final void setJobName(String name) {
        this.name = name;
    }

    /**
     * @param input To add to the input files for the job
     */
    public final void addInput(final File input) {
        inputFiles.add(input);
    }

    public final void setFirstIterationInputFromDisk(boolean bool) {
        this.firstIterationInputFromDisk = bool;
    }

    public final boolean getFirstIterationInputFromDisk() {
        return firstIterationInputFromDisk;
    }

    public final void setReducerRecordReader(final Class<? extends RecordReader> rreader) {
        this.reducerRecordReaderClass = rreader;
    }

    public final Class<? extends RecordReader> getReducerRecordReader() {
        return this.reducerRecordReaderClass;
    }

    public final void setIterationOutputToDisk(boolean bool, int endIter) {
        this.iterationOutputToDisk = bool;
        this.endIter = endIter;
    }

    public final boolean getIterationOutputToDisk() {
        return this.iterationOutputToDisk;
    }

    public final int getEndIter() {
        return endIter;
    }

    public final boolean getFirstIterationOutputToDisk() {
        return iterationOutputToDisk;
    }

    public final void setInputDirectoryPath(String inputDirectoryPath) {
        this.inputDirectoryPath = inputDirectoryPath;
    }

    public final String getInputDirectoryPath() {
        return inputDirectoryPath;
    }

    public final void setInMemoryInput(boolean bool) {
        this.inMemoryInput = bool;
    }

    public final boolean getInMemoryInput() {
        return inMemoryInput;
    }

    public final void setInMemoryOutput(boolean bool) {
        this.inMemoryOutput = bool;
    }

    public final boolean getInMemoryOutput() {
        return inMemoryOutput;
    }

    public final void setIterationNo(int iterNo) {
        this.iterationNo = iterNo;
    }

    public final int getIterationNo() {
        return iterationNo;
    }
    // getters and setters follow

    public final String getName() {
        return name;
    }

    public final Configuration getConfiguration() {
        return config;
    }

    public final void setVariable(String varName, Object variable) {
        if (variable != null) {
            this.varMap.put(varName, variable);
        }
    }

    public final Object getVariable(String varName) {
        return this.varMap.get(varName);
    }

    public final ArrayList<Object> getVariableList(String matchPartKey) {
        ArrayList list = new ArrayList<Object>();
        Object[] keys = varMap.keySet().toArray();
        for (int i = 0; i < keys.length; ++i) {
            if (((String) keys[i]).contains(matchPartKey) == true) {
                list.add(varMap.get((String) keys[i]));
            }
        }

        return list;
    }

    public final void remVariableList(String matchPartKey) {
        final Object[] keys = varMap.keySet().toArray();
        for (int i = 0; i < keys.length; ++i) {
            if (((String) keys[i]).contains(matchPartKey)) {
                varMap.remove((String) keys[i]);
            }
        }
    }

    public final void remVariable(String varName) {
        this.varMap.remove(varName);
    }

    public final void setMapperClass(final Class<? extends Mapper> mapperClass) {
        this.mapperClass = mapperClass;
    }

    public final Class<? extends Mapper> getMapperClass() {
        return mapperClass;
    }

    public final void setOutputFile(final File output) {
        this.outputFile = output;
    }

    public final File getOutputFile() {
        return outputFile;
    }

    public final List<File> getInputFiles() {
        return inputFiles;
    }

    public final Class<? extends InputFormat> getInputFormat() {
        return inputFormat;
    }

    public final void setInputFormatClass(final Class<? extends InputFormat> inputFormat) {
        this.inputFormat = inputFormat;
    }

    public final Class<? extends OutputFormat> getOutputFormat() {
        return outputFormat;
    }

    public final void setOutputFormatClass(final Class<? extends OutputFormat> outputFormat) {
        this.outputFormat = outputFormat;
    }

    public final Class<? extends Reducer> getReducerClass() {
        return reducerClass;
    }

    public final void setCombinerClass(final Class<? extends Combiner> combinerClass) {
        this.combinerClass = combinerClass;
    }

    public final Class<? extends Combiner> getCombinerClass() {
        return combinerClass;
    }

    public final void setReducerClass(final Class<? extends Reducer> reducerClass) {
        this.reducerClass = reducerClass;
    }

    public final Class<? extends Partitioner> getPartitionerClass() {
        return partitionerClass;
    }

    public final void setPartitionerClass(final Class<? extends Partitioner> partitionerClass) {
        this.partitionerClass = partitionerClass;
    }

    public final void setNumMapTasks(final int num) {
        this.numMapperWorkers = num;
    }

    public final int getNumMapTasks() {
        return this.numMapperWorkers;
    }

    public final void setNumReduceTasks(final int num) {
        this.numReducerWorkers = num;
    }

    public final int getNumReduceTasks() {
        return this.numReducerWorkers;
    }

    public final void setReduceOutput(List<Future<Object>> reduceOutput) {
        this.reduceOutput = reduceOutput;
    }

    public final List<Future<Object>> getReduceOutput() {
        return this.reduceOutput;
    }

    public final void setMapInput(List<Future<Object>> reduceOutput) {
        this.reduceOutput = reduceOutput;
    }

    public final List<Future<Object>> getMapInput() {
        return this.reduceOutput;
    }

    public final void setMapInputFromReducer(boolean boolValue) {
        this.mapInputFromReducer = boolValue;
    }

    public final boolean getMapInputFromReducer() {
        return this.mapInputFromReducer;
    }

    public final void setMapInputKeyClass(final Class<? extends WritableComparable> mapInputKeyClass) {
        this.mapKeyInputClass = mapInputKeyClass;
    }

    public final void setMapInputValueClass(final Class<? extends WritableComparable> mapInputValueClass) {
        this.mapValueInputClass = mapInputValueClass;
    }

    public final void setMapOutputKeyClass(final Class<? extends WritableComparable> mapOutputKeyClass) {
        this.mapKeyOutputClass = mapOutputKeyClass;
    }

    public final void setMapOutputValueClass(final Class<? extends WritableComparable> mapOutputValueClass) {
        this.mapOutputValueClass = mapOutputValueClass;
    }

    public final Class<? extends WritableComparable> getMapOutputKeyClass() {
        return this.mapKeyOutputClass;
    }

    public final Class<? extends WritableComparable> getMapOutputValueClass() {
        return this.mapOutputValueClass;
    }

    public final Class<? extends WritableComparable> getMapInputKeyClass() {
        return this.mapKeyInputClass;
    }

    public final Class<? extends WritableComparable> getMapInputValueClass() {
        return this.mapValueInputClass;
    }

    public final void setOutputKeyClass(final Class<? extends WritableComparable> outputKeyClass) {
        this.outputKeyClass = outputKeyClass;
    }

    public final void setOutputValueClass(final Class<? extends WritableComparable> outputValueClass) {
        this.outputValueClass = outputValueClass;
    }
}
