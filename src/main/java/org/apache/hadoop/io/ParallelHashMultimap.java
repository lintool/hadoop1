/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.io;

/**
 *
 * @author ashwinkayyoor
 */
public class ParallelHashMultimap {

    //private static final long NULL = 0L;

    private final WritableComparable[] keys;
    private final WritableComparable[] values;
    private int size;

    public ParallelHashMultimap(int capacity) {
        keys = new WritableComparable[capacity];
        values = new WritableComparable[capacity];
    }

    public void put(WritableComparable key, WritableComparable value) {
        if (key == null) throw new IllegalArgumentException("key cannot be NULL");
        if (size == keys.length) throw new IllegalStateException("map is full");

        int index = indexFor(key);
        while (keys[index] != null) {
            index = successor(index);
        }
        keys[index] = key;
        values[index] = value;
        ++size;
    }

    public WritableComparable[] get(WritableComparable key) {
        if (key == null) throw new IllegalArgumentException("key cannot be NULL");

        int index = indexFor(key);
        int count = countHits(key, index);

        WritableComparable[] hits = new WritableComparable[count];
        int hitIndex = 0;

        while (keys[index] != null) {
            if (keys[index] == key) {
                hits[hitIndex] = values[index];
                ++hitIndex;
            }
            index = successor(index);
        }

        return hits;
    }

    private int countHits(WritableComparable key, int index) {
        int numHits = 0;
        while (keys[index] != null) {
            if (keys[index] == key) ++numHits;
            index = successor(index);
        }
        return numHits;
    }

    private int indexFor(WritableComparable key) {
        // the hashing constant is (the golden ratio * Long.MAX_VALUE) + 1
        // see The Art of Computer Programming, section 6.4
        // the constant has two important properties:
        // (1) it is coprime with 2^64, so multiplication by it is a bijective function, and does not generate collisions in the hash
        // (2) it has a 1 in the bottom bit, so it does not add zeroes in the bottom bits of the hash, and does not generate (gratuitous) collisions in the index
        long hash = key.hashCode() * 5700357409661598721L;
        return Math.abs((int) (hash % keys.length));
    }

    private int successor(int index) {
        return (index + 1) % keys.length;
    }

    public int size() {
        return size;
    }

}