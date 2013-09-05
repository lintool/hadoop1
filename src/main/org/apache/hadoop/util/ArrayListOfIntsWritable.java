/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

/**
 *
 * @author ashwinkayyoor
 */
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.hadoop.io.DynamicDirectByteBuffer;
import org.apache.hadoop.io.WritableComparable;

/**
 * Writable extension of the {@code ArrayListOfInts} class. This class provides
 * an efficient data structure to store a list of ints for MapReduce jobs.
 *
 * @author Jimmy Lin
 */
public class ArrayListOfIntsWritable extends ArrayListOfInts
        implements WritableComparable<ArrayListOfIntsWritable> {

    /**
     * Constructs an ArrayListOfIntsWritable object.
     */
    public ArrayListOfIntsWritable() {
        super();
    }

    /**
     * Constructs an empty list with the specified initial capacity.
     *
     * @param initialCapacity	the initial capacity of the list
     */
    public ArrayListOfIntsWritable(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Constructs a list populated with shorts in range [first, last).
     *
     * @param first the smallest short in the range (inclusive)
     * @param last the largest short in the range (exclusive)
     */
    public ArrayListOfIntsWritable(int first, int last) {
        super(first, last);
    }

    /**
     * Constructs a deep copy of the ArrayListOfIntsWritable object given as
     * parameter.
     *
     * @param other object to be copied
     */
    public ArrayListOfIntsWritable(ArrayListOfIntsWritable other) {
        super();
        size = other.size();
        array = Arrays.copyOf(other.getArray(), size);
    }

    /**
     * Constructs a list from an array. Defensively makes a copy of the array.
     *
     * @param arr source array
     */
    public ArrayListOfIntsWritable(int[] arr) {
        super(arr);
    }

    public static int readInt(final byte[] a, final int i) {
        //return ByteBuffer.wrap(Arrays.copyOfRange(a, i, j)).getInt();
        return a[i] << 24 | (a[i + 1] & 0x000000FF) << 16 | (a[i + 2] & 0x000000FF) << 8 | (a[i + 3] & 0x000000FF);
    }

    /**
     * Deserializes this object.
     *
     * @param in source for raw byte representation
     */
    public void readFields(DataInputStream in) throws IOException {
        this.clear();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            add(i, in.readInt());
        }
    }

    /**
     * Deserializes this object.
     *
     * @param in source for raw byte representation
     */
    public void readFields(byte[] a, int offset) throws IOException {
        this.clear();
        int size = readInt(a, offset);
        for (int i = 0; i < size; i++) {
            offset += 4;
            add(i, readInt(a, offset));
        }
    }

    /**
     * Serializes this object.
     *
     * @param out	where to write the raw byte representation
     */
    @Override
    public int write(DataOutputStream out) throws IOException {
        int size = size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            out.writeInt(get(i));
        }

        return 4 * (size + 1);
    }

    @Override
    public int write(ByteBuffer buf) {
        int size = size();
        buf.putInt(size);
        for (int i = 0; i < size; i++) {
            buf.putInt(get(i));
        }

        return 4 * (size + 1);
    }

     @Override
    public int write(DynamicDirectByteBuffer buf) {
        int size = size();
        buf.putInt(size);
        for (int i = 0; i < size; i++) {
            buf.putInt(get(i));
        }

        return 4 * (size + 1);
    }

    @Override
    public String toString() {
        return toString(size());
    }

    /**
     * Creates a Writable version of this list.
     */
    public static ArrayListOfIntsWritable fromArrayListOfInts(ArrayListOfInts a) {
        ArrayListOfIntsWritable list = new ArrayListOfIntsWritable();
        list.array = Arrays.copyOf(a.getArray(), a.size());
        list.size = a.size();

        return list;
    }

    /**
     * Elementwise comparison. Shorter always comes before if it is a sublist of
     * longer. No preference if both are empty.
     *
     * @param obj other object this is compared against
     */
    @Override
    public int compareTo(ArrayListOfIntsWritable obj) {
        ArrayListOfIntsWritable other = (ArrayListOfIntsWritable) obj;
        if (isEmpty()) {
            if (other.isEmpty()) {
                return 0;
            } else {
                return -1;
            }
        }

        for (int i = 0; i < size(); i++) {
            if (other.size() <= i) {
                return 1;
            }
            if (get(i) < other.get(i)) {
                return -1;
            } else if (get(i) > other.get(i)) {
                return 1;
            }
        }

        if (other.size() > size()) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public int compare(byte[] a, int i1, int j1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getOffset() {
        return 0;
    }

    @Override
    public void set(WritableComparable obj) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
