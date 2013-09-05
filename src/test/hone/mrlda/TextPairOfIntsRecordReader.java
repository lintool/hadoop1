/**
 *
 */
package test.hone.mrlda;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.lib.output.RecordReader;
import org.apache.hadoop.io.PairOfInts;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.TreeMultiMap;

/**
 * This is a BS class at the moment
 *
 * @author tim
 */
public class TextPairOfIntsRecordReader extends RecordReader<Text, PairOfInts> {

    protected DataInputStream is;
    //protected ByteBuffer buffer;
    private Text k;
    private PairOfInts v;
    //private Iterator iterator;
    private byte[] input;
    private int offset;
    //private int length;
    //protected char[] buffer;
    private StringBuilder builder;
    private byte[] inputOffsets;
    private int inputOffsetsSize;
    //String str = "";
    //Map.Entry<WritableComparable, WritableComparable> entry;

    @Override
    public Text getCurrentKey() {
//        if (k.getLength() == 0) {
//            return null;
//        }
        return k;
    }

    @Override
    public String getKeyDataValue() {
        return k.toString();
    }

    @Override
    public PairOfInts getCurrentValue() {
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

    public final void initialize(final byte[] input, final byte[] inputOffsets, final int inputOffsetSize) {
        // make a read only copy just in case
        is = new DataInputStream(new BufferedInputStream(new ByteArrayInputStream(inputOffsets)));
        this.input = input;
        this.inputOffsets = inputOffsets;
        this.inputOffsetsSize = inputOffsetSize;
        builder = new StringBuilder(100);
    }

    @Override
    public void initialize(final TreeMultiMap<WritableComparable, WritableComparable> input) {
//        iterator = input.entrySet().iterator();
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
            k = new Text();
            v = new PairOfInts();
            //k.readFields(is);
            //v.readFields(is);
            if (is.available() > 0) {
                offset = is.readInt();
//                if (offset == 0) {
//                    return false;
//                }

            } else {
                return false;
            }

            k.set(readString(input, offset, offset + input[offset - 1] - 1));
      
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
