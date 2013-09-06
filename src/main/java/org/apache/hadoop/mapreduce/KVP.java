/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author ashwinkayyoor
 */
public class KVP {

    public WritableComparable k;
    public WritableComparable v;

    public KVP(KVP kvp) {
    }

    public KVP(WritableComparable k, WritableComparable v) {
        this.k = k;
        this.v = v;
    }
}
