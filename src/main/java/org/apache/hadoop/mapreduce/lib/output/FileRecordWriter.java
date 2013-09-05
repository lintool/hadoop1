/**
 *
 */
package org.apache.hadoop.mapreduce.lib.output;

import java.io.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.FastByteArrayOutputStream;

/**
 * Simplest of simple
 *
 * @author tim
 */
public class FileRecordWriter<KEY_OUT extends WritableComparable> extends RecordWriter {
    //protected Logger logger = Logger.getLogger(this.getClass().getName());

    private DataOutputStream output;
    private DataOutputStream outputTemp;
    private FileOutputStream fileoutput;
    private FastByteArrayOutputStream baos;

    public FileRecordWriter(final String s) throws IOException {
        final File f = new File(s);
        if (!f.exists()) {
            f.createNewFile();
        }
        output = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
        baos = new FastByteArrayOutputStream();
        outputTemp = new DataOutputStream(new BufferedOutputStream(baos));
        fileoutput = new FileOutputStream(f);
    }

    /**
     * closes the file
     */
    @Override
    public void close() throws IOException, InterruptedException {
        output.close();
        outputTemp.flush();

//        baos.writeTo(fileoutput);
//        baos.flush();
//        outputTemp.close();
//        baos.close();
        //baos.free();
    }

    /**
     * writes to the file
     */
    @Override
    public void write(final WritableComparable key, final WritableComparable value) throws IOException, InterruptedException {
//        key.write(outputTemp);
//        value.write(outputTemp);
        key.write(output);
        value.write(output);
    }

//    @Override
//    public SortedMultiMap getData() {
//        throw new UnsupportedOperationException("Not supported yet.");
//    }
    @Override
    public byte[] getData() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public byte[] getOffsets() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public byte[][] getDataArray() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public byte[][] getOffsetsArray() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
   
    @Override
    public byte[] getOutputByteArray() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int[] getElemCountArray() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    

    @Override
    public int getElemCount() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
