package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.apache.hadoop.mapreduce.lib.output.RecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

/**
 * The base Reducer class which does an Identity function by default.
 */
public abstract class Reducer<KEYIN extends WritableComparable, VALUEIN extends WritableComparable, KEYOUT extends WritableComparable, VALUEOUT extends WritableComparable> {

    private int reducerID;
    private Partitioner partitioner;
    private int numReducerWorkers;
    private byte[] outputByteArray;

    public class Context extends ReducerContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

        public Context(final KeyValuesIterator<KEYIN, VALUEIN> input, final RecordWriter<KEYOUT, VALUEOUT> writer, Job job) {
            super(input, writer, job);
        }
    }

    /**
     * Called once at the start of the task.
     */
    protected void setup(final Context context) throws IOException, InterruptedException {
    }

    /**
     * Called once at the start of the task.
     */
    protected void close() throws IOException {
    }

    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     */
    protected void reduce(final KEYIN key, final Iterable<VALUEIN> values, final Context context) throws InterruptedException, IOException {
        for (VALUEIN value : values) {
            context.write((KEYOUT) key, (VALUEOUT) value);
        }
    }

    /**
     * Set parameters
     */
    protected final void setParams(final int reducerID, final Partitioner partitioner, final int numReducerWorkers) throws IOException, InterruptedException {
        this.reducerID = reducerID;
        this.partitioner = partitioner;
        this.numReducerWorkers = numReducerWorkers;
    }

    /**
     * Called once at the end of the task.
     */
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }

    /**
     * Runs the reduce over the keys
     */
    public void run(final Context context, final Properties prop) throws IOException, InterruptedException, InstantiationException, IllegalAccessException {
        //setup(context);

        int partition = 0;
        final int arch = Integer.parseInt(prop.getProperty("arch"));
        setup(context);
        while (context.nextKey(partitioner, numReducerWorkers, reducerID)) {
            //System.out.println(context.getCurrentKey());
            if (arch == 1) {
                partition = partitioner.getPartition(context.getCurrentKey(), null, numReducerWorkers);
                // System.out.println("Key: "+context.getCurrentKey()+", Partition: "+partition+", reducerID: "+reducerID+", numReducerWorkers: "+numReducerWorkers);
                if (partition == reducerID) {
                    //System.out.println("KeyPassed: "+context.getCurrentKey());
                    //context.collectValuesForKey();
                    reduce(context.getCurrentKey(), context.getValues(), context);
                    //close();
                    //System.out.println("Key: "+context.getCurrentKey()+", Partition: "+partition+", reducerID: "+reducerID+", numReducerWorkers: "+numReducerWorkers+" values: "+context.getValues());
                }
            } else if (arch == 2 || arch == 3 || arch == 4) {
                reduce(context.getCurrentKey(), context.getValues(), context);
            }
            cleanup(context);
        }
        close();

        context.close();
        outputByteArray = context.getOutputByteArray();
    }

    public final byte[] getOutputByteArray() {
        return outputByteArray;
    }

    public final ByteBuffer getOutputByteBuffer() {
        return ByteBuffer.wrap(outputByteArray);
    }
}