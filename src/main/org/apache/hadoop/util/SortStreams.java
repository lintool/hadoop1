/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author ashwinkayyoor
 */
public class SortStreams implements Callable {

    protected byte[] dataArray;
    protected byte[] offsetArray;
    protected Object[] returnData = new Object[2];
    protected int totalElements;
    protected ByteQuickSort bqs;
    protected Job job;
    private Object[] intermArray;
    private int id;

    //protected ByteMergeSort bms;
    public SortStreams(final byte[] dataArray, final byte[] offsetArray, final int totalElements, Job job, Object[] intermArray, final int id) throws IOException, InstantiationException, IllegalAccessException {
        this.dataArray = dataArray;
        this.offsetArray = offsetArray;
        this.totalElements = totalElements;
        this.bqs = new ByteQuickSort(job);
        this.intermArray = intermArray;
        this.id = id;
    }

    @Override
    public Object call() {
        try {
            //System.out.println("dataArray size: " + dataArray.length + " offsetArray size: " + offsetArray.length + " totalElements: " + totalElements);

            bqs.quickSort(dataArray, offsetArray, 0, ((totalElements) / 4) - 1);
            // returnData[0] = dataArray;
            //returnData[1] = offsetArray;
        } catch (IOException ex) {
            //Logger.getLogger(SortStreams.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        }

        intermArray[this.id * 3 + 0] = dataArray;
        intermArray[this.id * 3 + 1] = offsetArray;
        intermArray[this.id * 3 + 2] = totalElements;

        return returnData;
    }
}
