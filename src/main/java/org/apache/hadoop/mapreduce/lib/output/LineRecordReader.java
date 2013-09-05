/**
 *
 */
package org.apache.hadoop.mapreduce.lib.output;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Iterator;
import java.util.regex.Matcher;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.TreeMultiMap;
import sun.nio.ch.DirectBuffer;

/**
 * @author tim
 */
public class LineRecordReader extends RecordReader<WritableComparable, WritableComparable> {

    private ByteBuffer buffer;
    private StringBuilder sb;
    // TODO 
    // need to do something sensible here.
    // Hadoop splits the data during submission so knows the number of rows upfront
    // since it can ask for the number of splits
    // We on the other hand chunk into blocks for mappers, and then split into records in 
    // parallel before we ever know how many rows are in a chunk.  An ordered row number is therefore
    // not possible to deduce
    private static long rowNumber = 0;
    private Text rowText;
    protected Iterator iterator;
    private byte[] barray;
    protected int bufferLength;
    protected BufferedReader br;
    protected String line;
    protected ByteArrayInputStream bstream;
    public static final char lineTerminator = '\n';
    private final static int MASK = 0x000000FF;
    private String[] tokens;
    private int ind = 0;
    private Matcher lineMatcher;

    @Override
    public LongWritable getCurrentKey() {
        return new LongWritable(rowNumber);
    }

    @Override
    public Text getCurrentValue() {
        return rowText;
    }

    @Override
    public void initialize(ByteBuffer input) {
        // make a read only copy just in case
        input.rewind();
        buffer = input;
        rowText = new Text();
        sb = new StringBuilder();
    }

    @Override
    public void initialize(byte[] input) {
        // make a read only copy just in case
        barray = input;
        rowText = new Text();
        sb = new StringBuilder();
        bufferLength = barray.length;
        bstream = new ByteArrayInputStream(barray);
        br = new BufferedReader(new InputStreamReader(bstream));
    }

    public static void unmap(MappedByteBuffer buffer) {
        sun.misc.Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
        cleaner.clean();
    }

    @Override
    public final boolean nextKeyValue() {

        // For each line
        while (buffer.hasRemaining()) {
            final char value = (char) (buffer.get() & MASK);
   
            if (lineTerminator == value || !buffer.hasRemaining()) {
                if (!buffer.hasRemaining()) {
                    sb.append(value);
                }

                rowNumber++;
                rowText.set(sb.toString());
                sb.setLength(0);
                return true;
            } else {
                sb.append(value);
            }
        }

        return false;
    }

    @Override
    public void initialize(final TreeMultiMap<WritableComparable, WritableComparable> input) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean nextKeyValue(final int ind) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object getKeyDataValue() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void initialize(byte[] input, byte[] offsets, int offsetSize) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
