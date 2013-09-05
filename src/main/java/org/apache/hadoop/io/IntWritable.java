/**
 *
 */
package org.apache.hadoop.io;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.util.ByteUtil;

/**
 * @author tim
 */
public class IntWritable implements WritableComparable {

    protected int i;

    public IntWritable() {
    }

    public IntWritable(int i) {
        this.i = i;
    }

    @Override
    public final void readFields(DataInputStream in) throws IOException {
        i = in.readInt();
    }

    @Override
    public final int write(DataOutputStream out) throws IOException {
        out.writeInt(i);
        return 4;
    }

    public final int write(ByteBuffer buf) {
        buf.putInt(i);
        return 4;
    }

    public final int write(DynamicDirectByteBuffer buf) {
        buf.putInt(i);
        return 4;
    }

    public final int get() {
        return i;
    }

    @Override
    public final int hashCode() {
        return i;
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final IntWritable other = (IntWritable) obj;
        if (this.i != other.i) {
            return false;
        }
        return true;
    }

    /**
     * Set the value of this IntWritable.
     */
    public final void set(int value) {
        this.i = value;
    }

    @Override
    public final int compareTo(Object o) {
        IntWritable t = (IntWritable) o;
        if (i == t.get()) {
            return 0;
        }
        if (i < t.get()) {
            return -1;
        }
        return 1;
    }

    @Override
    public final String toString() {
        return String.valueOf(i);
    }

    @Override
    public final int compare(final byte[] a, final int i1, final int j1) {

        int i1_len = 0, j1_len = 0;

        i1_len = 4;
        j1_len = 4;

        int ind1 = i1;
        int ind2 = j1;

        final int n = Math.min(i1_len, j1_len);
        int delta;
        for (int i = 0; i < n; ++i) {
            delta = a[ind1++] - a[ind2++];  // OK since bytes are smaller than ints.
            if (delta != 0) {
                return delta;
            }
        }

        delta = i1_len - j1_len;

        if (delta < 0) {
            return -1;
        } else if (delta != 0) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public final int getOffset() {
        return 0;
    }

    @Override
    public void set(WritableComparable obj) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Deserializes this object.
     *
     * @param in source for raw byte representation
     */
    public void readFields(byte[] a, int offset) throws IOException {
        this.i = ByteUtil.readInt(a, offset);
    }

    /**
     * Creates object from a
     * <code>DataInput</code>.
     *
     * @param in source for reading the serialized representation
     * @return newly-created object
     * @throws IOException
     */
    public static IntWritable create(DataInputStream in) throws IOException {
        IntWritable m = new IntWritable();
        m.readFields(in);

        return m;
    }

    /**
     * Creates object from a byte array.
     *
     * @param bytes raw serialized representation
     * @return newly-created object
     * @throws IOException
     */
    public static IntWritable create(byte[] bytes) throws IOException {
        return create(new DataInputStream(new ByteArrayInputStream(bytes)));
    }

    public IntWritable create(byte[] bytes, int offset) throws IOException {
        IntWritable m = new IntWritable();
        m.readFields(bytes, offset);
        return m;
    }
}