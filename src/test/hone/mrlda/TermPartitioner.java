package test.hone.mrlda;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.PairOfInts;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class TermPartitioner extends Partitioner<PairOfInts, DoubleWritable> {
  public int getPartition(PairOfInts key, DoubleWritable value, int numReduceTasks) {
    return (key.getLeftElement() & Integer.MAX_VALUE) % numReduceTasks;
  }

  public void configure(Job job) {
  }
}