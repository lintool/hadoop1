/**
 *
 */
package org.apache.hadoop.mapreduce.lib.input;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.hadoop.mapreduce.lib.output.RecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

/**
 * An input format is required for each type of input file, and will typically
 * come with a record reader
 *
 * MapReduce4J differs from Hadoop here as an input format is required to offer
 * the ability to read from an arbitrary byte stream and report the bytes read
 * to get to the next record split. This means that MapReduce4J will not
 * generate the Splits into memory, but instead allow the splits offsets be
 * identified in the input files, such that the input files can be split at
 * record boundaries and passed into the mappers. A record reader still provides
 * the record oriented view to the data.
 *
 * @author tim
 */
public abstract class InputFormat<KEY extends WritableComparable, VALUE extends WritableComparable> {

    public abstract long bytesUntilRecord(FileChannel fcin, long startByte) throws IOException;

    public abstract RecordReader<KEY, VALUE> createRecordReader(ByteBuffer byteBuffer);

    public abstract RecordReader<KEY, VALUE> createRecordReader(byte[] byteBuffer);

    public static void addInputPath(Job job, Path path) {
        job.addInput(new File(path.toString()));
    }

    public static void setInputPaths(Job job, Path path) {
        job.addInput(new File(path.toString()));
    }

    public abstract void initialize(Job job);

    public abstract ByteBuffer getByteBuffer(File file);
}
