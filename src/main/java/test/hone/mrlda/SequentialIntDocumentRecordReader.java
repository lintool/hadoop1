/**
 *
 */
package test.hone.mrlda;

import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.TreeMultiMap;

/**
 * @author tim
 */
public class SequentialIntDocumentRecordReader extends RecordReader<WritableComparable, WritableComparable> {

    private ByteBuffer buffer;
    private StringBuilder sb;
    private DataInputStream is;
    // TODO 
    // need to do something sensible here.
    // Hadoop splits the data during submission so knows the number of rows upfront
    // since it can ask for the number of splits
    // We on the other hand chunk into blocks for mappers, and then split into records in 
    // parallel before we ever know how many rows are in a chunk.  An ordered row number is therefore
    // not possible to deduce
    private static long rowNumber = 0;
    private Text rowText;
    //protected RecordParser recParser;
    protected Iterator iterator;
    private byte[] barray;
    private int index;
    protected int bufferLength;
    protected BufferedReader br;
    protected String line;
    protected ByteArrayInputStream bstream;
    public static final char lineTerminator = '\n';
    private IntWritable key = null;
    private Document value = null;
    private Job job;

    public SequentialIntDocumentRecordReader(Job job) {
        this.job = job;
    }

    @Override
    public IntWritable getCurrentKey() {
        return key;
    }

    @Override
    public Document getCurrentValue() {
        return value;
    }

    @Override
    public void initialize(ByteBuffer input) {
        // make a read only copy just in case
        //buffer = input.asReadOnlyBuffer();
        byte[] b = new byte[input.remaining()];
        input.get(b, 0, b.length);
        is = new DataInputStream(new BufferedInputStream(new ByteArrayInputStream(b)));
        key = new IntWritable(-1);
        value = new Document();
    }

    @Override
    public void initialize(byte[] input) {
        // make a read only copy just in case
        //buffer = input.asReadOnlyBuffer();
        barray = input;
        rowText = new Text();
        sb = new StringBuilder();
        //index = 0;
        bufferLength = barray.length;
        bstream = new ByteArrayInputStream(barray);
        br = new BufferedReader(new InputStreamReader(bstream));
    }

    @Override
    public boolean nextKeyValue() {
        try {
            if (is.available() > 0) {
                key.readFields(is);
                value.readFields(is);
                return true;
            }
        } catch (IOException ex) {
            Logger.getLogger(SequentialIntDocumentRecordReader.class.getName()).log(Level.SEVERE, null, ex);
        }

        return false;
    }

    @Override
    public void initialize(TreeMultiMap<WritableComparable, WritableComparable> input) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean nextKeyValue(int ind) {
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
