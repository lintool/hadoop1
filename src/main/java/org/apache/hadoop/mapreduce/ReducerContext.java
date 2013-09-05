/**
 *
 */
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.mapreduce.lib.output.RecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

/**
 * The context passed to the reducer
 *
 * @author tim
 */
public abstract class ReducerContext<KEY_IN extends WritableComparable, VALUE_IN extends WritableComparable, KEY_OUT extends WritableComparable, VALUE_OUT extends WritableComparable> {

    protected RecordWriter<KEY_OUT, VALUE_OUT> output;
    protected KeyValuesIterator<KEY_IN, VALUE_IN> input;
    protected RecordWriter<KEY_OUT, VALUE_OUT> writer;
    protected Configuration config;
    private Job job;

    public ReducerContext(final KeyValuesIterator<KEY_IN, VALUE_IN> input, final RecordWriter<KEY_OUT, VALUE_OUT> writer, Job job) {
        this.input = input;
        this.writer = writer;
        this.config = job.getConfiguration();
        this.job = job;
    }

    public void write(final KEY_OUT key, final VALUE_OUT value) throws IOException, InterruptedException {
        //System.out.println(key + ": " + value);
        writer.write(key, value);
        //output.write(key, value);
    }

    public final boolean nextKey() {
        return input.nextKeyValues();
    }

    public final boolean nextKey(final Partitioner partitioner, final int numReducers, final int reducerID) throws InstantiationException, IllegalAccessException {
        return input.nextKeyValues(partitioner, numReducers, reducerID);
    }

    public final Iterable<VALUE_IN> getValues() {
        return input.currentValues;
    }

    public final KEY_IN getCurrentKey() {
        return input.getCurrentKey();
    }

    public final Job getJobObject() {
        return job;
    }

    /**
     * @return the job configuration
     */
    public Configuration getConfiguration() {
        return config;
    }

    /**
     * Logs the status
     *
     * @param status to log
     */
    public void setStatus(String status) {
        //log.info(status);
    }

    public void close() throws IOException, InterruptedException {
        writer.close();
    }

    public byte[] getOutputByteArray() {
        return writer.getOutputByteArray();
    }

    public ByteBuffer getOutputByteBuffer() {
        return ByteBuffer.wrap(writer.getOutputByteArray());
    }
}
