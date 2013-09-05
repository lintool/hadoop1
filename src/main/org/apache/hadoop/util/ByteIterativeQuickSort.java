/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

import java.util.Arrays;

/**
 *
 * @author ashwinkayyoor
 */
public class ByteIterativeQuickSort {

    private static class SortRange {

        /**
         * Left index
         */
        int left;
        /**
         * Right index
         */
        int right;

        /**
         * Constructor
         */
        public SortRange(int left, int right) {
            this.left = left;
            this.right = right;
        }

        /**
         * Returns the size of the range, assuming that left <= right. We
         * specifically do not want the absolute value, that is Math.abs( right
         * - left ) as a case where right - left <= 1 is disregarded by the
         * algorithm.
         */
        public int size() {
            return right - left;
        }
    }

    private static int compare(byte[] a, int i1, int j1) {

        int i1_len = a[i1 - 1] & 0x000000FF;
        int j1_len = a[j1 - 1] & 0x000000FF;

        int n = Math.min(i1_len, j1_len);
        int delta;
        for (int i = 0; i < n; ++i) {
            delta = a[i1++] - a[j1++];  // OK since bytes are smaller than ints.
            if (delta != 0) {
                return delta;
            }
        }

        delta = i1_len - j1_len;
        return delta < 0 ? -1 : delta != 0 ? 1 : 0;
    }

    public static int fromByteArray(byte[] b, int start) {
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

    public static void swap(byte[] a, int i1, int i2, int j1, int j2) {
        int len = i2 - i1;

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

    public static void copy(byte[] b, int i1, int i2) {
        b[i1++] = b[i2++];
        b[i1++] = b[i2++];
        b[i1++] = b[i2++];
        b[i1] = b[i2];
    }

    public static String readString(byte[] a, int i) {
        int len = a[i - 1] & 0x000000FF;
        return new String(Arrays.copyOfRange(a, i, i + len));
    }

    private static int partition(byte[] a, byte[] b, int left, int right) {

        // Select pivot element
        //int pivot = array[ right];
        int pivot = fromByteArray(b, 4 * right);

        int i = left - 1;
        for (int j = left; j < right; j++) {
            if (compare(a, fromByteArray(b, (4 * j)), pivot) <= 0) {
                i++;
                swap(b, (4 * i), (4 * i) + 4, (4 * j), (4 * j) + 4);
            }
        }

        // Move the pivot element in the middle of the array
        //swap(array, i + 1, right);
        swap(b, (4 * (i + 1)), (4 * (i + 1)) + 4, (4 * right), (4 * right) + 4);

        // Return the pivot element index
        return i + 1;
    }

    private static int randPartition(byte[] a, byte[] b, int left, int right) {
        int randPivot = left + (int) (Math.random() * ((right - left) + 1));
        //System.out.println("randPivot: " + randPivot);
        //System.out.println("Swapping: " + readString(a, fromByteArray(b, (4 * right))) + " and " + readString(a, fromByteArray(b, (4 * randPivot))));
        swap(b, (4 * right), (4 * right) + 4, (4 * randPivot), (4 * randPivot) + 4);
        //swap( array, right, randPivot );
        return partition(a, b, left, right);
    }

    /**
     * Helper method for iterativeRandSort( int[ ], SortRange )
     */
    public static void iterativeRandSort(byte[] a, byte[] b, int i1, int j1) {
        iterativeRandSort(a, b, new SortRange(0, j1));
    }

    /**
     * Sorts the specified array iteratively based on randomized partitioning
     */
    private static void iterativeRandSort(byte[] a, byte[] b, SortRange range) {

        java.util.Stack<SortRange> ranges = new java.util.Stack<SortRange>();
        ranges.push(range);

        while (!ranges.isEmpty()) {

            SortRange currentRange = ranges.pop();
            if (currentRange.size() <= 1) {
                // Nothing to sort. Move to the next range.
                continue;
            }

            // Get the pivot index via randomized partitioning.
            int pivot = randPartition(a, b, currentRange.left, currentRange.right);
            //int pivot = partition(a, b, currentRange.left, currentRange.right);

            ranges.push(new SortRange(currentRange.left, pivot - 1));
            ranges.push(new SortRange(pivot + 1, currentRange.right));
        }
    }
}
