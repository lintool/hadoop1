/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

/**
 *
 * @author ashwinkayyoor
 */
public class StringTokenizer {
    private static ThreadLocal<String[]> tempArray = new ThreadLocal<String[]>();

    public static String[] tokenize(String string, char delimiter) 
    {
        String[] temp = tempArray.get();
        int tempLength = (string.length() / 2) + 2;

        if (temp == null || temp.length < tempLength) 
        {
            temp = new String[tempLength];
            tempArray.set(temp);
        }


        int wordCount = 0;
        int i = 0;
        int j = string.indexOf(delimiter);

        while (j >= 0)
        {
            temp[wordCount++] = string.substring(i, j);
            i = j + 1;
            j = string.indexOf(delimiter, i);
        }

        temp[wordCount++] = string.substring(i);

        String[] result = new String[wordCount];
        System.arraycopy(temp, 0, result, 0, wordCount);
        return result;
    }
}