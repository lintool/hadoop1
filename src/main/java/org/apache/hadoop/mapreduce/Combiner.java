/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.mapreduce.lib.output.RecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author ashwinkayyoor
 */
public abstract class Combiner<KEYIN extends WritableComparable, VALUEIN extends WritableComparable, KEYOUT extends WritableComparable, VALUEOUT extends WritableComparable> {

    private int reducerID;
    private Partitioner partitioner;
    private int numReducerWorkers;
    private byte[] dataArray;
    private byte[] offsetsArray;
    private static int id=0;

    public class Context extends CombinerContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

        public Context(final KeyValuesIterator<KEYIN, VALUEIN> input, final RecordWriter<KEYOUT, VALUEOUT> writer, Configuration config) {
            super(input, writer, config);
        }
    }

    /**
     * Called once at the start of the task.
     */
    protected void setup(final Combiner.Context context) throws IOException, InterruptedException {
    }

    /**
     * Called once at the end of the task.
     */
    protected void close() throws IOException {
    }

    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     */
    protected void reduce(final KEYIN key, final Iterable<VALUEIN> values, final Combiner.Context context) throws IOException, InterruptedException {
        for (VALUEIN value : values) {
            context.write((KEYOUT) key, (VALUEOUT) value);
        }
    }

    /**
     * Set parameters
     */
    protected void setParams(final int reducerID, final Partitioner partitioner, final int numReducerWorkers) throws IOException, InterruptedException {
        this.reducerID = reducerID;
        this.partitioner = partitioner;
        this.numReducerWorkers = numReducerWorkers;
    }

    /**
     * Called once at the end of the task.
     */
    protected void cleanup(Combiner.Context context) throws IOException, InterruptedException {
    }

    /**
     * Runs the reduce over the keys
     */
    public void run(final Context context, final Properties prop) throws IOException, InterruptedException, InstantiationException, IllegalAccessException {
        //setup(context);
        System.out.println("Combiner id: "+id++);
        while (context.nextKey(partitioner, numReducerWorkers, reducerID)) {
            reduce(context.getCurrentKey(), context.getValues(), context);
        }
        context.close();
        close();

        dataArray = context.getDataArray();
        offsetsArray = context.getOffsetArray();
    }

    public final byte[] getDataArray() {
        return dataArray;
    }

    public final byte[] getOffsetsArray() {
        return offsetsArray;
    }
}
