/**
 *
 */
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.InputFormat;
import org.apache.hadoop.mapreduce.lib.output.RecordReader;
import org.apache.hadoop.mapreduce.lib.output.RecordWriter;

/**
 * The base functionality for the context passed around the Job
 *
 * @author tim
 */
public abstract class MapContext<KEY_IN extends WritableComparable, VALUE_IN extends WritableComparable, KEY_OUT extends WritableComparable, VALUE_OUT extends WritableComparable> {
    // might we consider making this static? 
    // if so we would loose the class name, or have to delegate to 
    // subclasses a 'getLogger()' method

    //protected Logger logger = Logger.getLogger(this.getClass().getName());
    private RecordReader<KEY_IN, VALUE_IN> reader;
    // job config
    private Configuration config;
    private RecordWriter<KEY_OUT, VALUE_OUT> writer;
    private Job job;
    //protected ArrayList linesList;

    public MapContext(final InputFormat inputFormat, final RecordWriter<KEY_OUT, VALUE_OUT> writer, final ByteBuffer bb, Job job) throws IOException {
//        final Buffer rewind = bb.rewind();
        this.reader = inputFormat.createRecordReader(bb);
        this.writer = writer;
        this.config = job.getConfiguration();
        this.job = job;
    }

    public MapContext(final InputFormat inputFormat, final RecordWriter<KEY_OUT, VALUE_OUT> writer, final byte[] bb) throws IOException {
        this.reader = inputFormat.createRecordReader(bb);
        this.writer = writer;
    }


    /**
     * Logs the status
     *
     * @param status to log
     */
    public void setStatus(String status) {
        //logger.log(Level.INFO, status);
    }

    /**
     * @return the job configuration
     */
    public final Configuration getConfiguration() {
        return config;
    }
    
    public final Job getJobObject(){
        return job;
    }

    /**
     * @param key to write to the job output
     * @param value to write to the output
     */
    public final void write(final KEY_OUT key, final VALUE_OUT value) {
        try {
            writer.write(key, value);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public final byte[] getData() {
        return writer.getData();
    }

    public final byte[] getOffsets() {
        return writer.getOffsets();
    }

    public final byte[][] getDataArray() {
        return writer.getDataArray();
    }

    public final byte[][] getOffsetsArray() {
        return writer.getOffsetsArray();
    }

    public final int[] getElemCountArray() {
        return writer.getElemCountArray();
    }

    public final KEY_IN getCurrentKey() throws IOException, InterruptedException {
        return reader.getCurrentKey();
    }

    public final VALUE_IN getCurrentValue() throws IOException,
            InterruptedException {
        return reader.getCurrentValue();
    }

    public final boolean nextKeyValue() throws IOException, InterruptedException {
        return reader.nextKeyValue();
    }

    public final void close() throws IOException, InterruptedException {
        writer.close();
    }
}
