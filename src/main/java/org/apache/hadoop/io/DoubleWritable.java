/**
 *
 */
package org.apache.hadoop.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.util.ByteUtil;

/**
 * @author tim
 */
public class DoubleWritable implements WritableComparable {

    protected double i;

    public DoubleWritable() {
    }

    public DoubleWritable(double i) {
        this.i = i;
    }

    @Override
    public void readFields(DataInputStream in) throws IOException {
        i = in.readDouble();
    }

    @Override
    public int write(DataOutputStream out) throws IOException {
        out.writeDouble(i);
        return 8;
    }

    public int write(ByteBuffer buf) {
        buf.putDouble(i);
        return 8;
    }

    public int write(DynamicDirectByteBuffer buf) {
        buf.putDouble(i);
        return 8;
    }

    public final double get() {
        return i;
    }

    @Override
    public int hashCode() {
        return ((Double) i).hashCode();
    }

    /**
     * Set the value of this IntWritable.
     */
    public final void set(double value) {
        this.i = value;
    }

    @Override
    public int compareTo(Object o) {
        DoubleWritable t = (DoubleWritable) o;
        if (i == t.get()) {
            return 0;
        }
        if (i < t.get()) {
            return -1;
        }
        return 1;
    }

    @Override
    public String toString() {
        return String.valueOf(i);
    }

    @Override
    public int compare(byte[] a, int i1, int j1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public final int getOffset() {
        return 0;
    }

    @Override
    public void set(WritableComparable obj) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void readFields(byte[] input, int offset) throws IOException {
        this.i = ByteUtil.readDouble(input, offset);
    }

    @Override
    public DoubleWritable create(byte[] input, int offset) throws IOException {
        DoubleWritable m = new DoubleWritable();
        m.readFields(input, offset);
        return m;
    }
}