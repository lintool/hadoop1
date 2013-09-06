/**
 * 
 */
package org.apache.hadoop.mapreduce.lib.output;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;


/**
 * @author tim
 *
 */
public abstract class OutputFormat<K extends WritableComparable, V extends WritableComparable> {
	public abstract RecordWriter<K, V> getRecordWriter(Job job) throws IOException, InterruptedException;
        public abstract RecordWriter<K, V> getRecordWriter(Job job, Properties prop) throws IOException, InterruptedException;
	//public abstract OutputCommitter getOutputCommitter() throws IOException, InterruptedException;
        public static void setOutputPath(Job job, Path path) {
        job.setOutputFile(new File(path.toString()));
    }
}