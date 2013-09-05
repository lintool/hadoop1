/**
 *
 */
package org.apache.hadoop.mapreduce.lib.output;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ByteUtil;
import org.apache.hadoop.util.TreeMultiMap;

/**
 * This is a BS class at the moment
 *
 * @author tim
 */
public class KeyValueRecordReader extends RecordReader<WritableComparable, WritableComparable> {

    protected DataInputStream is;
    private WritableComparable k;
    private WritableComparable v;
    private Iterator iterator;
    private byte[] input;
    private int offset;
    private int offsetArrayIndex;
    private StringBuilder builder;
    private byte[] inputOffsets;
    private int inputOffsetsSize;
    private int previousKeyEle;
    private int currentKeyEle;
    private final static int MASK = 0x000000FF;
    private Job job;

    @Override
    public WritableComparable getCurrentKey() {
        return k;
    }

    @Override
    public String getKeyDataValue() {
        return k.toString();
    }

    @Override
    public final WritableComparable getCurrentValue() {
        return v;
    }

    @Override
    public final void initialize(final byte[] input) {
    }

    private String decodeUTF16BE(final byte[] bytes, int startByte, final int byteCount) {
        final StringBuilder builder = new StringBuilder(byteCount);
        for (int i = 0; i < byteCount; i++) {
            final byte low = bytes[startByte++];
            final int ch = (low & MASK);
            builder.append((char) ch);
        }

        return builder.toString();
    }

    public String readString(final byte[] a, final int i, final int j) throws CharacterCodingException {
        return decodeUTF16BE(a, i, (j - i) + 1);
    }

    public static int fromByteArray(final byte[] a, final int i) {
        return a[i] << 24 | (a[i + 1] & MASK) << 16 | (a[i + 2] & MASK) << 8 | (a[i + 3] & MASK);
    }

    public final void initialize(final byte[] input, final byte[] inputOffsets, int inputOffsetsSize, Job job) {
        // make a read only copy just in case
        this.input = input;
        this.inputOffsets = inputOffsets;
        this.inputOffsetsSize = inputOffsetsSize;
        this.job = job;
        builder = new StringBuilder(100);
        offsetArrayIndex = 0;
        try {
            this.k = job.getMapOutputKeyClass().newInstance();
            this.v = job.getMapOutputValueClass().newInstance();
        } catch (InstantiationException ex) {
            Logger.getLogger(KeyValueRecordReader.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(KeyValueRecordReader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void initialize(final TreeMultiMap<WritableComparable, WritableComparable> input) {
        iterator = input.entrySet().iterator();
    }

    @Override
    public final boolean nextKeyValue() {
        try {
            if (k == null) {
                k = new IntWritable(-1);
            }
            v = new Text("");

            if (offsetArrayIndex != inputOffsetsSize) {
                offset = ByteUtil.readInt(inputOffsets, offsetArrayIndex);
                offsetArrayIndex += 4;
            } else {
                input = null;
                inputOffsets = null;
                return false;
            }

            currentKeyEle = ByteUtil.readInt(input, offset);
            if (previousKeyEle != -1 && (previousKeyEle != currentKeyEle)) {
                k = new IntWritable(currentKeyEle);
            }
            previousKeyEle = currentKeyEle;

            k.set(currentKeyEle);
            v.set(readString(input, offset + 4 + v.getOffset(), offset + 4 + v.getOffset() + (input[offset + 4 + v.getOffset() - 1] & MASK) - 1));

            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public final boolean nextKeyValue(final int ind) {
        try {
            if (ind == inputOffsets.length) {
                input = null;
                inputOffsets = null;
                return false;
            }

            k = new IntWritable(-1);
            v = new Text("");

            offset = ByteUtil.readInt(inputOffsets, ind);
            k.set(ByteUtil.readInt(input, offset));
            v.set(readString(input, offset + 4 + k.getOffset(), input[offset + 4 + k.getOffset() - 1] & MASK));

            return true;
        } catch (Exception e) { //e.printStackTrace(); return false;
        }
        return false;
    }

    // Returns an input stream for a ByteBuffer.
    // The read() methods use the relative ByteBuffer get() methods.
    public static InputStream newInputStream(final ByteBuffer buf) {
        return new InputStream() {

            @Override
            public synchronized int read() throws IOException {
                if (!buf.hasRemaining()) {
                    return -1;
                }
                return buf.get();
            }

            @Override
            public synchronized int read(final byte[] bytes, final int off, int len) throws IOException {
                // Read only what's left
                len = Math.min(len, buf.remaining());
                buf.get(bytes, off, len);
                return len;
            }
        };
    }

    @Override
    public void initialize(final ByteBuffer input) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
