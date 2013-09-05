/**
 *
 */
package test.hone.pagerank;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.output.RecordReader;
import org.apache.hadoop.util.ByteUtil;
import org.apache.hadoop.util.TreeMultiMap;

/**
 * This is a BS class at the moment
 *
 * @author tim
 */
public class IntPageRankNodeRecordReader extends RecordReader<IntWritable, PageRankNode> {

    protected DataInputStream is;
    private IntWritable k;
    private PageRankNode v;
    private Iterator iterator;
    private byte[] input;
    private int offset;
    private StringBuilder builder;
    private byte[] inputOffsets;
    private int inputOffsetsSize;
    private int offsetArrayIndex;
    private int previousKeyEle;
    private int currentKeyEle;
    private final static int MASK = 0x000000FF;

    @Override
    public IntWritable getCurrentKey() {
        return k;
    }

    @Override
    public Integer getKeyDataValue() {
        return k.get();
    }

    @Override
    public PageRankNode getCurrentValue() {
        return v;
    }

    @Override
    public void initialize(final byte[] input) {
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

    public final String readString(final byte[] a, final int i, final int j) {
        return decodeUTF16BE(a, i, (j - i) + 1);
    }

    public static int readInt(final byte[] a, final int i) {
        return a[i] << 24 | (a[i + 1] & 0x000000FF) << 16 | (a[i + 2] & 0x000000FF) << 8 | (a[i + 3] & 0x000000FF);
    }

    public static int fromByteArray(final byte[] a, final int i) {
        return a[i] << 24 | (a[i + 1] & 0x000000FF) << 16 | (a[i + 2] & 0x000000FF) << 8 | (a[i + 3] & 0x000000FF);
    }

    public final void initialize(final byte[] input, final byte[] inputOffsets, final int inputOffsetsSize) {
        // make a read only copy just in case
        //is = new DataInputStream(new BufferedInputStream(new ByteArrayInputStream(inputOffsets)));
        this.input = input;
        this.inputOffsets = inputOffsets;
        this.inputOffsetsSize = inputOffsetsSize;
        offsetArrayIndex = 0;
        previousKeyEle = -1;
    }

    @Override
    public void initialize(final TreeMultiMap<WritableComparable, WritableComparable> input) {
        iterator = input.entrySet().iterator();
    }

    @Override
    public boolean nextKeyValue() {
        try {
            if (k == null) {
                k = new IntWritable(-1);
            }
            //v = new PageRankNode();

            if (offsetArrayIndex != inputOffsetsSize) {
                offset = ByteUtil.readInt(inputOffsets, offsetArrayIndex);
                offsetArrayIndex += 4;
            } else {
                return false;
            }

            currentKeyEle = readInt(input, offset);
            if (previousKeyEle != -1 && (previousKeyEle != currentKeyEle)) {
                k = new IntWritable(currentKeyEle);
            }
            previousKeyEle = currentKeyEle;

            k.set(currentKeyEle);
            v = v.create(input, offset + 4);

            return true;
        } catch (Exception e) {
            e.printStackTrace();
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
            public synchronized int read(byte[] bytes, int off, int len) throws IOException {
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

    @Override
    public boolean nextKeyValue(int ind) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
