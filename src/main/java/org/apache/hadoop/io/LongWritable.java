/**
 *
 */
package org.apache.hadoop.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author tim
 */
public class LongWritable implements WritableComparable {

    protected long l;

    public LongWritable() {
    }

    public LongWritable(long l) {
        this.l = l;
    }

    @Override
    public final void readFields(DataInputStream in) throws IOException {
        l = in.readLong();
    }

    @Override
    public final int write(DataOutputStream out) throws IOException {
        out.writeLong(l);
        return 4;
    }

    public final int write(ByteBuffer buf) {
        buf.putLong(l);
        return 4;
    }

    public final int write(DynamicDirectByteBuffer buf) {
        buf.putLong(l);
        return 4;
    }

    public final long get() {
        return l;
    }

    public final void set(long l) {
        this.l = l;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LongWritable) {
            return l == ((LongWritable) o).get();
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 53 * hash + (int) (this.l ^ (this.l >>> 32));
        return hash;
    }

    @Override
    public int compareTo(Object o) {
        final LongWritable t = (LongWritable) o;
        if (l == t.get()) {
            return 0;
        }
        if (l < t.get()) {
            return -1;
        }
        return 1;
    }

    @Override
    public final String toString() {
        return String.valueOf(l);
    }

    @Override
    public final int compare(byte[] a, int i1, int j1) {
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
}
