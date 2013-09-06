/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author ashwinkayyoor
 */
public class ByteQuickSort {

    private Properties prop;
    private Job job;
    private WritableComparable keyClass;
    //static FileInputStream fis;

    public ByteQuickSort(Job job) throws IOException, InstantiationException, IllegalAccessException {
        this.job = job;
        keyClass = job.getMapOutputKeyClass().newInstance();
        //this.prop = new Properties();
        //fis = new FileInputStream("config.properties");
        //prop.load(fis);
    }

    private int compare(final byte[] a, final int i1, final int j1) {

        int i1_len = 0, j1_len = 0;
//        if (prop.getProperty("application").equals("wordcount")) {
//            i1_len = a[i1 - 1] & 0x000000FF;
//            j1_len = a[j1 - 1] & 0x000000FF;
//        } else if (prop.getProperty("application").equals("pagerank")) {
//            i1_len = 4;
//            j1_len = 4;
//        }

        i1_len = 4;
        j1_len = 4;

        int ind1 = i1;
        int ind2 = j1;

        final int n = Math.min(i1_len, j1_len);
        int delta;
        for (int i = 0; i < n; ++i) {
            delta = a[ind1++] - a[ind2++];  // OK since bytes are smaller than ints.
            if (delta != 0) {
                return delta;
            }
        }

        delta = i1_len - j1_len;

        if (delta < 0) {
            return -1;
        } else if (delta != 0) {
            return 1;
        } else {
            return 0;
        }

        //return delta < 0 ? -1 : delta != 0 ? 1 : 0;
    }

    public static byte[] getByteArray(final byte[] a, final int i) {
        byte[] b = {a[i], a[i + 1], a[i + 2], a[i + 3]};
        return b;
    }

    public static int fromByteArray(final byte[] b, final int start) {

        // System.out.println("");
        int value = 0;
        int ind = 0;
        int shift;
        for (int i = start; i < start + 4; i++) {
            shift = (4 - 1 - ind) * 8;
            value += (b[i] & 0x000000FF) << shift;
            ind++;
        }
        return value;
    }

//    public static int fromByteArray(byte[] a, int i) {
//        //return ByteBuffer.wrap(Arrays.copyOfRange(a, i, i+4)).getInt();
//        return a[i] << 28 | (a[i + 1] & 0xFF) << 16 | (a[i + 2] & 0xFF) << 8 | (a[i + 3] & 0xFF);
//    }
    private void swap(final byte[] a, final int i1, final int i2, final int j1, final int j2) {
        final int len = i2 - i1;

        int i = i1;
        int j = j1;

        byte temp;
        for (int ind = 0; ind < len; ++ind) {

            if (a[i] != a[j]) {
                temp = a[i];
                a[i] = a[j];
                a[j] = temp;
            }
            i++;
            j++;

        }
    }

//    private int randPartition(byte[] a, byte[] b, int left, int right) {
//        int randPivot = left + (int) (Math.random() * ((right - left) + 1));
//        //System.out.println("randPivot: " + randPivot);
//        //System.out.println("Swapping: " + readString(a, fromByteArray(b, (4 * right))) + " and " + readString(a, fromByteArray(b, (4 * randPivot))));
//        swap(b, (4 * right), (4 * right) + 4, (4 * randPivot), (4 * randPivot) + 4);
//        //swap( array, right, randPivot );
//        return partition(a, b, left, right);
//    }
    private int partition(final byte[] a, final byte[] b, final int left, final int right) {
        int i = left, j = right;
        //int tmp;
        final int center = (left + right) / 2;
        //byte[] pivot = getByteArray(b, 4 * center);
        final int pivot = fromByteArray(b, 4 * center);

        while (i <= j) {

            while (keyClass.compare(a, fromByteArray(b, (4 * i)), pivot) < 0) {
                i++;
            }
            while (keyClass.compare(a, fromByteArray(b, (4 * j)), pivot) > 0) {
                j--;
            }
            if (i <= j) {
                if (i != j) {
                    swap(b, (4 * i), (4 * i) + 4, (4 * j), (4 * j) + 4);
                }
                i++;
                j--;
            }
        };
        return i;
    }

    public final void quickSort(final byte[] a, final byte[] b, final int left, final int right) throws IOException {
        //System.out.println(right+" a_len: "+a.length+" b_len: "+b.length);
        //System.exit(0);
        if (a.length > 0) {
            final int index = partition(a, b, left, right);
            if (left < index - 1) {
                quickSort(a, b, left, index - 1);
            }
            if (index < right) {
                quickSort(a, b, index, right);
            }
        }
        //fis.close();
    }
}
