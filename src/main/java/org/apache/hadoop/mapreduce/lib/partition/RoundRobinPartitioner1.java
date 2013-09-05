/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.mapreduce.lib.partition;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author ashwinkayyoor
 */
public class RoundRobinPartitioner1<K, V> extends Partitioner<K, V> {

    int partitionNo = 0;

    @Override
    public int getPartition(K key, V value, int numPartitions) {
        int returnPartition = (partitionNo++) % numPartitions;
        if (partitionNo == numPartitions) {
            partitionNo = 0;
        }
        return returnPartition;
    }
}
