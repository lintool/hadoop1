/**
 *
 */
package org.apache.hadoop.mapreduce.lib.input;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.RecordReader;
import org.apache.hadoop.mapreduce.lib.output.SequentialFileRecordReader;

/**
 * An input format for reading text files delimited by \n
 *
 * @author tim
 */
public class NonSplitableSequenceMemoryInputFormat extends InputFormat implements Callable {
    // bytes to read at a time from the input

    public static final int READ_AHEAD_BYTES = 256;
    protected ByteBuffer buffer;
    public static final char lineTerminator = '\n';
    private Job job;

    public NonSplitableSequenceMemoryInputFormat(ByteBuffer input) {
        // make a read only copy just in case
        this.buffer = input;
        this.buffer.rewind();
    }

    public NonSplitableSequenceMemoryInputFormat() {
        // make a read only copy just in case
    }

    /**
     * @param fcin To read from
     * @param startByte To start at
     * @return The number of bytes read until a new line is encountered
     * @throws IOException On file reading error
     */
    @Override
    public long bytesUntilRecord(FileChannel fcin, long startByte) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(READ_AHEAD_BYTES);

        boolean found = false;
        long offset = 0;
        // loop in case the READ_AHEAD_BYTES is not enough to reach the end
        while (!found) {
            bb.rewind();

            // need to stop if we are passed the end of file
            // TODO - we are missing something here!!!
            // the end of the file...
            if (startByte >= fcin.size()) {
                break;
            }

            fcin.read(bb, startByte + offset);
            for (int i = 0; i < bb.limit(); i++) {
                offset++;
                // note we read bytes, not chars
                if ((char) bb.get(i) == '\n') {
                    found = true;
                    break;
                }
            }
        }

        // uhm... do we need to rewind the BB
        //bb.rewind();

        return offset;
    }

    @Override
    public RecordReader<WritableComparable, WritableComparable> createRecordReader(ByteBuffer byteBuffer) {
        RecordReader reader = new SequentialFileRecordReader(job);
        reader.initialize(byteBuffer);
        return reader;
    }

    public RecordReader<WritableComparable, WritableComparable> createRecordReader(byte[] byteBuffer) {
        LineRecordReader reader = new LineRecordReader();
        reader.initialize(byteBuffer);
        return reader;
    }

    @Override
    public Object call() throws Exception {

        String str = "";
        List<String> strArray = new ArrayList();
        int i = 0;
        System.out.println("Going in");
        //Buffer rewind = buffer.rewind();

        String line;
        StringBuilder sb = new StringBuilder();
        while (buffer.hasRemaining()) {
            char value = (char) buffer.get();
            if (lineTerminator == value || !buffer.hasRemaining()) {
                line = sb.toString();
                strArray.add(line);
                //System.out.println(line);
                sb.setLength(0);
            } else {
                sb.append(value);
            }
        }

        return strArray;
    }

    @Override
    public ByteBuffer getByteBuffer(File file) {
        FileChannel roChannel = null;

        try {
            roChannel = new RandomAccessFile(file, "r").getChannel();
        } catch (FileNotFoundException ex) {
        }

        ByteBuffer roBuf = null;
        try {
            roBuf = roChannel.map(FileChannel.MapMode.READ_ONLY, 0, (int) roChannel.size());
        } catch (IOException ex) {
        }

        return roBuf;
    }

    @Override
    public void initialize(Job job) {
        this.job = job;
    }
}
