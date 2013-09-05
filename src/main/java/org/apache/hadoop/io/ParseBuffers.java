/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.io;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 *
 * @author ashwinkayyoor
 */
public class ParseBuffers implements Callable {

    protected ByteBuffer buffer;
    public static final char lineTerminator = '\n';
    StringBuilder sb;

    public ParseBuffers(ByteBuffer input) {
        // make a read only copy just in case
        this.buffer = input;
        this.buffer.rewind();
        sb = new StringBuilder();

    }

    public String nextKeyValue() {
        System.out.println("Here: " + buffer.array().length);
        while (buffer.hasRemaining()) {
            //System.out.println("Yes");
            char value = (char) buffer.get();
            if (lineTerminator == value || !buffer.hasRemaining()) {
                if (!buffer.hasRemaining()) {
                    sb.append(value);
                }
                String line = sb.toString();
                sb.setLength(0);
                //System.out.println(line);

                //k = new Text(parts[0]);
                return line;
                //return true;
            } else {
                sb.append(value);
            }
        }
        return null;
    }

    @Override
    public Object call() throws Exception {

        String str = "";
        List<String> strArray = new ArrayList();
        int i = 0;
        System.out.println("Going in");
        //Buffer rewind = buffer.rewind();

        String line;
        StringBuilder sb = new StringBuilder();
        while (buffer.hasRemaining()) {
            char value = (char) buffer.get();
            if (lineTerminator == value || !buffer.hasRemaining()) {
                line = sb.toString();
                strArray.add(line);
                //System.out.println(line);
                sb.setLength(0);
            } else {
                sb.append(value);
            }
        }

        return strArray;

        /*
         * while (true) {
         *
         * String lineStr = null; StringBuilder sb = new StringBuilder();
         * System.out.println(buffer.capacity());
         *
         * while (buffer.hasRemaining()) { // System.out.println("Yes"); char
         * value = (char) buffer.get(); if (lineTerminator == value ||
         * !buffer.hasRemaining()) { if (!buffer.hasRemaining()) {
         * sb.append(value); } String line = sb.toString(); lineStr = line;
         * System.out.println(line);
         *
         * //k = new Text(parts[0]); break; //return true; } else {
         * sb.append(value); } } strArray[i++] = lineStr; }
         */

        /*
         * while ((str = nextKeyValue()) != null) { System.out.println(str);
         * strArray[i++] = str; }
         */

        //return strArray;
    }
}
