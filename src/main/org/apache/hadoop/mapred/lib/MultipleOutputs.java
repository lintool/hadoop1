/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.output.MemoryRecordWriter;
import org.apache.hadoop.mapreduce.lib.output.RecordWriter;

/**
 *
 * @author ashwinkayyoor
 */
public class MultipleOutputs {

    final RecordWriter writer;

    public MultipleOutputs() throws IOException {
        writer = new MemoryRecordWriter();
    }

    public MultipleOutputs(Configuration conf) throws IOException {
        writer = new MemoryRecordWriter();
    }

    public void close() throws IOException, InterruptedException {
        writer.close();
    }

    public OutputCollector getCollector(String namedOutput, String multiName, Reporter reporter) throws IOException {

        return new OutputCollector() {

            @Override
            public void collect(WritableComparable key, WritableComparable value) {
                try {
                    writer.write(key, value);
                } catch (IOException ex) {
                    Logger.getLogger(MultipleOutputs.class.getName()).log(Level.SEVERE, null, ex);
                } catch (InterruptedException ex) {
                    Logger.getLogger(MultipleOutputs.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        };
    }
    
     public OutputCollector getCollector(String namedOutput, String multiName) throws IOException {

        return new OutputCollector() {

            @Override
            public void collect(WritableComparable key, WritableComparable value) {
                try {
                    writer.write(key, value);
                } catch (IOException ex) {
                    Logger.getLogger(MultipleOutputs.class.getName()).log(Level.SEVERE, null, ex);
                } catch (InterruptedException ex) {
                    Logger.getLogger(MultipleOutputs.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        };
    }
}
