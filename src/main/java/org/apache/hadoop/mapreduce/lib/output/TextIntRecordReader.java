/**
 *
 */
package org.apache.hadoop.mapreduce.lib.output;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ByteUtil;
import org.apache.hadoop.util.TreeMultiMap;

/**
 * This is a BS class at the moment
 *
 * @author tim
 */
public class TextIntRecordReader extends RecordReader<Text, IntWritable> {

    protected DataInputStream is;
    //protected DataInputStream iStream;
    //protected ByteBuffer buffer;
    private Text k;
    private IntWritable v;
    //private Iterator iterator;
    private byte[] input;
    private int offset;
    private int offsetArrayIndex;
    //private int length;
    //protected char[] buffer;
    private StringBuilder builder;
    private byte[] inputOffsets;
    private int inputOffsetSize;
    private final static int MASK = 0x000000FF;
    private String previousKeyEle;
    private String currentKeyEle;
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
    public final IntWritable getCurrentValue() {
        return v;
    }

    @Override
    public final void initialize(final byte[] input) {
    }

    private String decodeUTF16BE(final byte[] bytes, int startByte, final int byteCount) {
        final StringBuilder builder = new StringBuilder(byteCount);
        //char[] buffer = new char[byteCount];
        //builder.delete(0, builder.length());
        //int sByte = startByte;

        for (int i = 0; i < byteCount; i++) {
            final byte low = bytes[startByte++];
            //byte high = bytes[startByte++];
            final int ch = (low & MASK);
            builder.append((char) ch);
            //buffer[i] = (char) ch;
        }

        return builder.toString();
        //return new String(buffer);
    }

    public String readString(final byte[] a, final int i, final int j) throws CharacterCodingException {
        //return new String(Arrays.copyOfRange(a, i, j + 1));
        //byte[] bytes = Arrays.copyOfRange(a, i, j+1);
        //return Text.decode(bytes);
        return decodeUTF16BE(a, i, (j - i) + 1);
    }

    public static int fromByteArray(final byte[] a, final int i) {
        //return ByteBuffer.wrap(Arrays.copyOfRange(a, i, j)).getInt();
        return a[i] << 24 | (a[i + 1] & MASK) << 16 | (a[i + 2] & MASK) << 8 | (a[i + 3] & MASK);
    }

    public final void initialize(final byte[] input, final byte[] inputOffsets, final int inputOffsetsSize) {
        // make a read only copy just in case
        //is = new DataInputStream(new ByteArrayInputStream(inputOffsets));
        //iStream = new DataInputStream(new ByteArrayInputStream(input));
        this.input = input;
        this.inputOffsets = inputOffsets;
        this.inputOffsetSize = inputOffsetSize;
        //     builder = new StringBuilder(100);
        offsetArrayIndex = 0;
        previousKeyEle = null;
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
    public final boolean nextKeyValue() {
        try {
            if (k == null) {
                k = new Text("");
            }
            if (v == null) {
                v = new IntWritable(-1);
            }
            //k.readFields(is);
            //v.readFields(is);
//            if (is.available() > 0) {
//                offset = is.readInt();
//                if (offset == 0) {
//                    return false;
//                }
//
//            } else {
//                return false;
//            }
            //if (offsetArrayIndex != inputOffsets.length) {
            if(offsetArrayIndex != inputOffsetSize){
                offset = ByteUtil.readInt(inputOffsets, offsetArrayIndex);
                offsetArrayIndex += 4;
            } else {
                //input = null;
                //inputOffsets = null;
                return false;
            }
            //length = is.readInt();

            //System.out.println("key read: " + readString(input, offset, offset + (input[offset - 1] & MASK) - 1) + ", " + fromByteArray(input, offset + (input[offset - 1] & MASK)));
            //str = readString(input, offset, offset + input[offset - 1] - 1);
            currentKeyEle = readString(input, offset, offset + (input[offset - 1] & MASK) - 1);
            if (previousKeyEle == null || previousKeyEle.equals(currentKeyEle) == false ) {
                k = new Text();
                k.set(currentKeyEle);
                v = new IntWritable(-1);
                v.set(fromByteArray(input, offset + (input[offset - 1] & MASK)));
            }
            previousKeyEle = currentKeyEle;
            //k.set(input, offset, (input[offset - 1] & 0x000000FF));


            //System.out.println(k.toString());

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
                is.close();
                is = null;
                //iStream.close();
                //iStream = null;
                return false;
            }

            k = new Text("");
            v = new IntWritable(-1);
            //k.readFields(is);
            //v.readFields(is);

            offset = fromByteArray(inputOffsets, ind);
            //length = is.readInt();

            //str = readString(input, offset, offset + input[offset - 1] - 1);
            k.set(readString(input, offset, offset + (input[offset - 1] & MASK) - 1));
            //k.set(input, offset, (input[offset - 1] & 0x000000FF));
            v.set(fromByteArray(input, offset + (input[offset - 1] & MASK)));

            //System.out.println(k.toString());

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
