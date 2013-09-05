/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.output.RecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author ashwinkayyoor
 */
public abstract class CombinerContext<KEY_IN extends WritableComparable, VALUE_IN extends WritableComparable, KEY_OUT extends WritableComparable, VALUE_OUT extends WritableComparable> {

    protected RecordWriter<KEY_OUT, VALUE_OUT> output;
    protected KeyValuesIterator<KEY_IN, VALUE_IN> input;
    protected RecordWriter<KEY_OUT, VALUE_OUT> writer;
    protected Configuration config;

    public CombinerContext(final KeyValuesIterator<KEY_IN, VALUE_IN> input, final RecordWriter<KEY_OUT, VALUE_OUT> writer, Configuration config) {
        this.input = input;
        this.writer = writer;
        this.config = config;
    }

    public final void write(final KEY_OUT key, final VALUE_OUT value) throws IOException, InterruptedException {
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

    /**
     * Logs the status
     *
     * @param status to log
     */
    public final void setStatus(String status) {
        //log.info(status);
    }

    public final byte[] getDataArray() {
        return writer.getData();
    }

    public final byte[] getOffsetArray() {
        return writer.getOffsets();
    }

    public final void close() throws IOException, InterruptedException {
        writer.close();
    }
}
