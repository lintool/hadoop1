/**
 *
 */
package org.apache.hadoop.mapreduce.lib.output;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.regex.Pattern;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.TreeMultiMap;

/**
 * Reads a KVP from a TAB file
 *
 * @author tim
 */
public class KVPRecordReader extends RecordReader<Text, Text> {

    protected ByteBuffer buffer;
    protected Iterator iterator;
    protected Text k;
    protected Text v;
    String[] kvPair;
    public static final char lineTerminator = '\n';
    protected static Pattern tab = Pattern.compile("\t");

    @Override
    public Text getCurrentKey() {
        return k;
    }

    @Override
    public Text getCurrentValue() {
        return v;
    }

    @Override
    public void initialize(ByteBuffer input) {
        // make a read only copy just in case
        buffer = input;
        k = new Text();
        v = new Text();
    }

    /*public void initialize(HashSet input) {
        // make a read only copy just in case
        iterator = input.iterator();
    }*/

    @Override
    public boolean nextKeyValue() {
        while (iterator.hasNext()) {

            kvPair = ((String) iterator.next()).split("$,");
            k.set(kvPair[0]);
            v.set(kvPair[1]);

            return true;
        }
        return false;
    }
    /*
     * public boolean nextKeyValue() { StringBuffer sb = new StringBuffer();
     * while (buffer.hasRemaining()) { char value = (char)buffer.get(); if
     * (lineTerminator == value || !buffer.hasRemaining()) { if
     * (!buffer.hasRemaining()) { sb.append(value); } String line =
     * sb.toString(); String parts[] = tab.split(line);
     *
     * k = new Text1(parts[0]); v = new Text1(parts[1]); return true; } else {
     * sb.append(value); } } return false; }
     */

    
    @Override
    public void initialize(byte[] input) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void initialize(TreeMultiMap<WritableComparable, WritableComparable> input) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean nextKeyValue(int ind) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object getKeyDataValue() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void initialize(byte[] input, byte[] offsets, int offsetSize) {
        throw new UnsupportedOperationException("Not supported yet.");
    }    
}
