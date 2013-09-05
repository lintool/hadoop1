/**
 *
 */
package org.apache.hadoop.mapreduce.lib.output;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.io.ByteArrayOutputStream_long;
import org.apache.hadoop.io.ByteArrayOutputStream_test;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

/**
 * Simplest of simple
 *
 * @author tim
 */
public class MemoryRecordWriter<KEY_OUT extends WritableComparable> extends RecordWriter {
    //protected Logger logger = Logger.getLogger(this.getClass().getName());

    private DataOutputStream output;
    private DataOutputStream outputTemp;
    private DataOutputStream oostream;
    //private FileOutputStream fileoutput;
    private ByteArrayOutputStream_test baos;
    private ByteArrayOutputStream_test ofostream;
    private Properties prop;
    private int bytesWritten;
    private int arch;
    private byte[] outputByteArray;
    private byte[] offsetsArray;
    private int totalElements;    

    public MemoryRecordWriter(Job job, Properties prop) throws IOException {
//        arch = Integer.parseInt(prop.getProperty("arch"));
        baos = new ByteArrayOutputStream_test();
        output = new DataOutputStream(new BufferedOutputStream(baos));
        ofostream = new ByteArrayOutputStream_test();
        oostream = new DataOutputStream(new BufferedOutputStream(ofostream));
    }

    public MemoryRecordWriter(Job job) throws IOException {
//        arch = Integer.parseInt(prop.getProperty("arch"));
        baos = new ByteArrayOutputStream_test();
        output = new DataOutputStream(new BufferedOutputStream(baos));
        ofostream = new ByteArrayOutputStream_test();
        oostream = new DataOutputStream(new BufferedOutputStream(ofostream));
    }

    public MemoryRecordWriter() throws IOException {
//        arch = Integer.parseInt(prop.getProperty("arch"));
        baos = new ByteArrayOutputStream_test();
        output = new DataOutputStream(new BufferedOutputStream(baos));
        ofostream = new ByteArrayOutputStream_test();
        oostream = new DataOutputStream(new BufferedOutputStream(ofostream));
    }

    /**
     * closes the file
     */
    @Override
    public void close() throws IOException, InterruptedException {
        output.flush();
        output.close();

        oostream.flush();
        oostream.close();
        ofostream.close();

        outputByteArray = baos.getBuf();
        offsetsArray = ofostream.getBuf();
        totalElements = ofostream.size();

        oostream = null;
        ofostream = null;
        baos = null;
        output = null;

        //System.out.println("MemoryRecordWriter size: Total----------" + MemoryUtil.deepMemoryUsageOf(this));
//        System.out.println("MemoryRecordWriter size: outputByteArray--"+MemoryUtil.deepMemoryUsageOf(outputByteArray));
//        System.out.println("MemoryRecordWriter size: offsetsArray--"+MemoryUtil.deepMemoryUsageOf(offsetsArray));
    }

    /**
     * writes to the file
     */
    @Override
    public void write(final WritableComparable key, final WritableComparable value) throws IOException, InterruptedException {
//        oostream.writeInt(bytesWritten);
        bytesWritten += key.write(output);
        bytesWritten += value.write(output);
        //System.out.println(key+" "+value);
    }

    @Override
    public final byte[] getData() {
        return outputByteArray;
    }

    @Override
    public final byte[] getOffsets() {
        return offsetsArray;
    }

    @Override
    public final byte[] getOutputByteArray() {
        return outputByteArray;
    }

    @Override
    public final byte[][] getOffsetsArray() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public final int getElemCount() {
        return totalElements;
    }

    @Override
    public final byte[][] getDataArray() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int[] getElemCountArray() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
