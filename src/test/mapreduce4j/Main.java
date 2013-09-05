/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test.mapreduce4j;

import test.hone.wordcount.WordCount;
import java.io.FileInputStream;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 *
 * @author ashwinkayyoor
 */
public class Main {
     public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        prop.load(new FileInputStream("config.properties"));

        //Logger.global.setLevel(Level.OFF);
        Configuration conf = new Configuration();
        Job job = new Job(conf, "word count");
        //job.setJarByClass(WordCount1.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(WordCount.IntSumReducer.class);
        job.setPartitionerClass(HashPartitioner.class);
        //job.addInput(new File(prop.getProperty("inputFilePath")));
        job.setInputFormatClass(TextInputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
