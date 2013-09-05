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
public class ByteQuickSort1 {

    private static int compare(byte[] a, int i1, int j1) {


        int i1_len = a[i1 - 1] & 0x000000FF;
        int j1_len = a[j1 - 1] & 0x000000FF;

        String str1 = readString(a, i1);
        String str2 = readString(a, j1);

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

    

//    public static int fromByteArray(byte[] b, int start) {
//        int value = 0;
//        int ind = 0;
//        int shift;
//        for (int i = start; i < start + 4; i++) {
//            shift = (4 - 1 - ind) * 8;
//            value += (b[i] & 0x000000FF) << shift;
//            ind++;
//        }
//        return value;
//    }
    public static int fromByteArray(byte[] a, int i) {
        //return ByteBuffer.wrap(Arrays.copyOfRange(a, i, i+4)).getInt();
        //System.out.println(i);
        return a[i] << 24 | (a[i + 1] & 0xFF) << 16 | (a[i + 2] & 0xFF) << 8 | (a[i + 3] & 0xFF);
    }

    private static void swap(byte[] a, int i1, int i2, int j1, int j2) {
        int len = i2 - i1;

        int i = i1;
        int j = j1;
//        int k = i1 + 4;
//        int l = j1 + 4;
        byte temp;
        for (int ind = 0; ind < len; ++ind) {

            if (a[i] != a[j]) {
                temp = a[i];
                a[i] = a[j];
                a[j] = temp;
            }
            i++;
            j++;

//            if (a[k] != a[l]) {
//                temp = a[k];
//                a[k] = a[l];
//                a[l] = temp;
//            }
//            k++;
//            l++;
        }
    }

    public static byte[] getByteArray(byte[] a, int i) {
        byte[] b = {a[i], a[i + 1], a[i + 2], a[i + 3]};
        return b;
    }

    public static String readString(byte[] a, int i) {
        int len = a[i - 1] & 0x000000FF;
        return new String(Arrays.copyOfRange(a, i, i + len));
    }

    private static int partition(byte[] a, byte[] b, int left, int right) {
        int i = left, j = right;
        int tmp;
        int center = (left + right) / 2;
        byte[] pivot = getByteArray(b, 4 * center);
        
        while (i < j) {

           // System.out.println("compare " + readString(a, fromByteArray(b, (4 * i))) + " (<) " + readString(a, fromByteArray(b, 4 * center)));
            while (compare(a, fromByteArray(b, (4 * i)), fromByteArray(pivot, 0)) < 0) {
             //   System.out.println("compare success" + readString(a, fromByteArray(b, (4 * i))) + " (<) " + readString(a, fromByteArray(b, 4 * center)));
                i++;
               // System.out.println(i);
            }
            //i++;
            //System.out.println("compare " + readString(a, fromByteArray(b, (4 * j))) + " (>) " + readString(a, fromByteArray(b, 4 * center)));
            while (compare(a, fromByteArray(b, (4 * j)), fromByteArray(pivot, 0)) > 0) {
              //  System.out.println("compare success" + readString(a, fromByteArray(b, (4 * j))) + " (>) " + readString(a, fromByteArray(b, 4 * center)));
                j--;
            }
            //j--;
            if (i <= j) {
                if (i != j) {
                //    System.out.println("Swapping: " + readString(a, fromByteArray(b, (4 * i))) + " and " + readString(a, fromByteArray(b, (4 * j))));
                    swap(b, (4 * i), (4 * i) + 4, (4 * j), (4 * j) + 4);
                }
                i++;
                j--;
            }
        };
        return i;
    }

    public static void quickSort(byte[] a, byte[] b, int left, int right) {
        //System.out.println(right+" a_len: "+a.length+" b_len: "+b.length);
        //System.exit(0);
        if (left >= right) {
            return;
        }
        int index = partition(a, b, left, right);
        //System.out.println("Index: " + index + " left: " + left + " right: " + right);
        if (left < index - 1) {
            quickSort(a, b, left, index - 1);
        }
        if (index < right) {
            quickSort(a, b, index, right);
        }
    }
}
