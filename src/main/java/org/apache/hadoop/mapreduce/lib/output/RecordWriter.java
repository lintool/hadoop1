/**
 *
 */
package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author tim
 *
 */
public abstract class RecordWriter<KEY_OUT extends WritableComparable, VALUE_OUT extends WritableComparable> {

    public abstract void write(KEY_OUT key, VALUE_OUT value) throws IOException, InterruptedException;

    public abstract void close() throws IOException, InterruptedException;

//    public abstract SortedMultiMap<WritableComparable, WritableComparable> getData();
    public abstract byte[] getData();

    public abstract byte[][] getDataArray();

    public abstract byte[] getOutputByteArray();

    public abstract byte[] getOffsets();

    public abstract byte[][] getOffsetsArray();

    public abstract int[] getElemCountArray();
    
    public abstract int getElemCount();
}
