/**
 *
 */
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.InputFormat;
import org.apache.hadoop.mapreduce.lib.output.RecordWriter;


/**
 * The base Mapper class which does an Identity function by default.
 *
 * @author tim
 */
public abstract class Mapper<KEY_IN extends WritableComparable, VALUE_IN extends WritableComparable, KEY_OUT extends WritableComparable, VALUE_OUT extends WritableComparable> {

    private byte[] data;
    private byte[] offsets;
    private byte[][] dataArray;
    private byte[][] offsetsArray;
    private int[] totalElements;
    private static final String METHOD_NAME = "map";
    //Properties prop;

    /**
     * Simple pass through (e.g. Identity function)
     *
     * @param key The input key
     * @param value The input value
     * @param context The job context
     * @throws IOException Should the context fail to write or bad key/value
     * types are observed
     */
    @SuppressWarnings("unchecked")
    protected void map(final KEY_IN key, final VALUE_IN value, final Context context) throws IOException, InterruptedException {
        //context.setStatus("Key[" + key + "], Value[" + value + "]");
        context.write((KEY_OUT) key, (VALUE_OUT) value);
    }

    /**
     * Does nothing
     */
    protected void setup(final Context context) throws IOException, InterruptedException {
    }

    /**
     * Close
     */
    protected void close() throws IOException, InterruptedException {
    }

    /**
     * This package declaration is required by Hadoop
     *
     * @author tim
     */
    public class Context extends MapContext<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> {

        public Context(final InputFormat inputFormat, final RecordWriter<KEY_OUT, VALUE_OUT> writer, final ByteBuffer bb, Job job) throws IOException {
            super(inputFormat, writer, bb, job);
        }

        public Context(final InputFormat inputFormat, final RecordWriter<KEY_OUT, VALUE_OUT> writer, final byte[] bb) throws IOException {
            super(inputFormat, writer, bb);
        }
        /*
         * public Context(RecordWriter<KEY_OUT, VALUE_OUT> writer, ArrayList
         * inputList) throws IOException { super(writer, inputList); }
         */
    }

    /**
     * Runs the map over the input
     */
    public final void run(final Context context, final int id, final Properties prop) throws IOException, InterruptedException, NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, Throwable {
        //setup(context);
        System.out.println("Mapper id: " + id);
        Configuration conf = context.getConfiguration();
        conf.setInt("map.task.id", id);

        //Method method = this.getClass().getMethod(METHOD_NAME, context.getCurrentKey().getClass(), context.getCurrentValue().getClass(), Context.class);
//        MethodType methodType;
//        MethodHandle methodHandle;
//        MethodHandles.Lookup lookup = MethodHandles.lookup();
//
//        methodType = MethodType.methodType(void.class, context.getCurrentKey().getClass(), context.getCurrentValue().getClass(), Context.class);
//        methodHandle = lookup.findVirtual(this.getClass(), METHOD_NAME, methodType);


        int arch = Integer.parseInt(prop.getProperty("arch"));
        setup(context);
        while (context.nextKeyValue()) {
            //System.out.println("nextValue: "+context.getCurrentValue());
            //method.invoke(this, context.getCurrentKey(), context.getCurrentValue(), context);
            //methodHandle.invoke(this, context.getCurrentKey(), context.getCurrentValue(), context);
            map(context.getCurrentKey(), context.getCurrentValue(), context);
            //System.out.println("Line: "+context.getCurrentValue().toString());
            //Thread.yield();
        }
        close();
        context.close();

        if (arch == 1) {
            data = context.getData();
            offsets = context.getOffsets();
        } else if (arch == 2) {
            dataArray = context.getDataArray();
            //System.out.println("Array: "+dataArray[3]);
            offsetsArray = context.getOffsetsArray();
            totalElements = context.getElemCountArray();
        } 
    }

    public final byte[] getData() {
        return data;
    }

    public final byte[] getOffsets() {
        return offsets;
    }

    public final byte[][] getDataArray() {
        return dataArray;
    }

    public final byte[][] getOffsetsArray() {
        return offsetsArray;
    }

    public final int[] getElemCount() {
        return totalElements;
    }

    public final byte[] run(final List lineList, final Context context) throws IOException, InterruptedException {
        System.out.println("Context key values");
        final Iterator iter = lineList.iterator();

        while (iter.hasNext()) {
            map(null, (VALUE_IN) (new Text((String) iter.next())), context);
            iter.remove();
        }

        context.close();
        return context.getData();
    }
}