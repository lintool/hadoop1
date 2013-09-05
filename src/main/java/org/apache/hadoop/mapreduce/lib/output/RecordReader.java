/**
 *
 */
package org.apache.hadoop.mapreduce.lib.output;

import java.nio.ByteBuffer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.TreeMultiMap;

/**
 * A record reader is required to generate the key and values for a record, and
 * also handle moving through a byte stream. Note: This differs from the Hadoop
 * implementation as Hadoop will generate InputSplits before feeding into the
 * reader. Here, we don't pre split the data, but just stream it and read as we
 * stream.
 *
 * @author tim
 */
public abstract class RecordReader<KEY_IN extends WritableComparable, VALUE_IN extends WritableComparable> {

    /**
     * @return The key from the current record
     */
    public abstract KEY_IN getCurrentKey();

    /**
     * @return The value from the current record
     */
    public abstract VALUE_IN getCurrentValue();

    /**
     * @param input File to read
     */
    public abstract void initialize(ByteBuffer input);

    public abstract void initialize(TreeMultiMap<WritableComparable, WritableComparable> input);

    public abstract void initialize(byte[] input);
    
    public abstract void initialize(byte[] input, byte[] offsets, int offsetsSize);


    /**
     * @return true if there was another key/value to read, otherwise false
     */
    public abstract boolean nextKeyValue();

    public abstract boolean nextKeyValue(int ind);

    public abstract Object getKeyDataValue();
}
