/**
 *
 */
package test.hone.mrlda;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.RecordReader;
import org.apache.hadoop.util.ByteUtil;
import org.apache.hadoop.util.TreeMultiMap;

/**
 * This is a BS class at the moment
 *
 * @author tim
 */
public class PairOfIntsDoubleRecordReader extends RecordReader<PairOfInts, DoubleWritable> {

    protected DataInputStream is;
    private PairOfInts k;
    private DoubleWritable v;
    private Iterator iterator;
    private byte[] input;
    private int offset;
    private StringBuilder builder;
    private byte[] inputOffsets;
    private int inputOffsetsSize;
    private int offsetArrayIndex;
    private PairOfInts previousKeyEle;
    private PairOfInts currentKeyEle;

    @Override
    public PairOfInts getCurrentKey() {
//        if (k.getLength() == 0) {
//            return null;
//        }
        return k;
    }

    @Override
    public Integer getKeyDataValue() {
        return null;
    }

    @Override
    public DoubleWritable getCurrentValue() {
        return v;
    }

    @Override
    public void initialize(final byte[] input) {
    }

    private String decodeUTF16BE(final byte[] bytes, final int startByte, final int byteCount) {
        //StringBuilder builder = new StringBuilder(byteCount);
        //char[] buffer = new char[byteCount];
        builder.delete(0, builder.length());
        int sByte = startByte;

        for (int i = 0; i < byteCount; i++) {
            final byte low = bytes[sByte++];
            //byte high = bytes[startByte++];
            int ch = (low & 0x000000FF);
            builder.append((char) ch);
            //buffer[i] = (char) ch;
        }

        return builder.toString();
        //return new String(buffer);
    }

    public final String readString(final byte[] a, final int i, final int j) {
        //return new String(Arrays.copyOfRange(a, i, j + 1));
        return decodeUTF16BE(a, i, (j - i) + 1);
    }

    public static int readInt(final byte[] a, final int i) {
        //return ByteBuffer.wrap(Arrays.copyOfRange(a, i, j)).getInt();
        return a[i] << 24 | (a[i + 1] & 0x000000FF) << 16 | (a[i + 2] & 0x000000FF) << 8 | (a[i + 3] & 0x000000FF);
    }

    public static int fromByteArray(final byte[] a, final int i) {
        //return ByteBuffer.wrap(Arrays.copyOfRange(a, i, j)).getInt();
        return a[i] << 24 | (a[i + 1] & 0x000000FF) << 16 | (a[i + 2] & 0x000000FF) << 8 | (a[i + 3] & 0x000000FF);
    }

    public final void initialize(final byte[] input, final byte[] inputOffsets, final int inputOffsetsSize) {
        // make a read only copy just in case
        is = new DataInputStream(new BufferedInputStream(new ByteArrayInputStream(inputOffsets)));
        this.input = input;
        this.inputOffsets = inputOffsets;
        this.inputOffsetsSize = inputOffsetsSize;
        builder = new StringBuilder(100);
    }

    @Override
    public void initialize(final TreeMultiMap<WritableComparable, WritableComparable> input) {
        iterator = input.entrySet().iterator();
    }

//    public boolean nextKeyValue() {
//
//        //System.out.println("has next: "+iterator.hasNext());
//        if (iterator.hasNext()) {
//            //System.out.println(KV);
//            entry = (Entry<WritableComparable, WritableComparable>) iterator.next();
//            k = new Text1(((Text1)entry.getKey()).toString());
//            v = new IntWritable(((IntWritable)entry.getValue()).get());
//            kvPair = null;
//            //iterator.remove();
//            //System.out.println("Here next key value " + k + ", " + v);
//            return true;
//
//        }
//        return false;
//    }
    @Override
    public boolean nextKeyValue() {
        try {
            if (k == null) {
                k = new PairOfInts();
            }
            v = new DoubleWritable();

            if (offsetArrayIndex != inputOffsetsSize) {
                offset = ByteUtil.readInt(inputOffsets, offsetArrayIndex);
                offsetArrayIndex += 4;
            } else {
                return false;
            }

            currentKeyEle = k.create(input, offset);
            if (previousKeyEle!=null && !previousKeyEle.equals(currentKeyEle)) {
                k = new PairOfInts(currentKeyEle.getLeftElement(), currentKeyEle.getRightElement());
            }
            previousKeyEle = currentKeyEle;

            k = currentKeyEle;

            v.set(ByteUtil.toDouble(input, offset + 4));

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
