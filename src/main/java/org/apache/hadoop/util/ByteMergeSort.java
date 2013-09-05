/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

import java.util.Arrays;

class ByteMergeSort {

    //protected static int totalElements = 0;
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

    public static void copy(byte[] a, byte[] b, int i1, int i2) {
//        if(i1>100)
//            System.out.println("here");
        a[i1++] = b[i2++];
        a[i1++] = b[i2++];
        a[i1++] = b[i2++];
        a[i1] = b[i2];
    }

    public static String readString(byte[] a, int i) {
        int len = a[i - 1] & 0x000000FF;
        return new String(Arrays.copyOfRange(a, i, i + len));
    }

    static public void DoMerge(byte[] a, byte[] b, int left, int mid, int right, int totalElements) {
        byte[] temp = new byte[(totalElements) * 4];
        int i, left_end, num_elements, tmp_pos;

        left_end = (mid - 1);
        tmp_pos = left;
        num_elements = (right - left + 1);

        while ((left <= left_end) && (mid <= right)) {
            if (compare(a, fromByteArray(b, (4 * left)), fromByteArray(b, (4 * mid))) <= 0) {
                copy(temp, b, tmp_pos * 4, left * 4);
                tmp_pos++;
                left++;
            } else {
                copy(temp, b, tmp_pos * 4, mid * 4);
                tmp_pos++;
                mid++;
            }
        }

        while (left <= left_end) {
            copy(temp, b, tmp_pos * 4, left * 4);
            tmp_pos++;
            left++;
        }

        while (mid <= right) {
            copy(temp, b, tmp_pos * 4, mid * 4);
            tmp_pos++;
            mid++;
        }

        for (i = 0; i < num_elements; i++) {
            copy(b, temp, right * 4, right * 4);
            right--;
        }
    }

    static public void MergeSort_Recursive(byte[] a, byte[] b, int left, int right) {
        int mid;

        int totalElements = right + 1;

        if (right > left) {
            mid = (right + left) / 2;
            MergeSort_Recursive(a, b, left, mid);
            MergeSort_Recursive(a, b, (mid + 1), right);

            DoMerge(a, b, left, (mid + 1), right, totalElements);
        }
    }
}