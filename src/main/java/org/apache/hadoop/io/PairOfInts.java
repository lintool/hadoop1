/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.hadoop.io;

import java.io.*;
import java.nio.ByteBuffer;
import org.apache.hadoop.util.ByteUtil;

/**
 * WritableComparable representing a pair of ints. The elements in the pair are
 * referred to as the left and right elements. The natural sort order is: first
 * by the left element, and then by the right element.
 *
 * @author Jimmy Lin
 */
public class PairOfInts implements WritableComparable<PairOfInts> {

    private int leftElement, rightElement;

    /**
     * Creates a pair.
     */
    public PairOfInts() {
    }

    /**
     * Creates a pair.
     *
     * @param left the left element
     * @param right the right element
     */
    public PairOfInts(int left, int right) {
        set(left, right);
    }

    public static int readInt(final byte[] a, final int i) {
        //return ByteBuffer.wrap(Arrays.copyOfRange(a, i, j)).getInt();
        return a[i] << 24 | (a[i + 1] & 0x000000FF) << 16 | (a[i + 2] & 0x000000FF) << 8 | (a[i + 3] & 0x000000FF);
    }

    /**
     * Deserializes this pair.
     *
     * @param in source for raw byte representation
     */
    public void readFields(DataInputStream in) throws IOException {
        leftElement = in.readInt();
        rightElement = in.readInt();
    }

    public void readFields(byte[] a, int offset) throws IOException {
        leftElement = readInt(a, offset);
        offset += 4;
        rightElement = readInt(a, offset);
    }

    /**
     * Serializes this pair.
     *
     * @param out where to write the raw byte representation
     */
    @Override
    public int write(DataOutputStream out) throws IOException {
        out.writeInt(leftElement);
        out.writeInt(rightElement);
        return 8;
    }

    @Override
    public int write(ByteBuffer buf) {
        buf.putInt(leftElement);
        buf.putInt(rightElement);
        return 8;
    }

    @Override
    public int write(DynamicDirectByteBuffer buf) {
        buf.putInt(leftElement);
        buf.putInt(rightElement);
        return 8;
    }
    
    /**
     * Returns the left element.
     *
     * @return the left element
     */
    public int getLeftElement() {
        return leftElement;
    }

    /**
     * Returns the right element.
     *
     * @return the right element
     */
    public int getRightElement() {
        return rightElement;
    }

    /**
     * Returns the key (left element).
     *
     * @return the key
     */
    public int getKey() {
        return leftElement;
    }

    /**
     * Returns the value (right element).
     *
     * @return the value
     */
    public int getValue() {
        return rightElement;
    }

    /**
     * Sets the right and left elements of this pair.
     *
     * @param left the left element
     * @param right the right element
     */
    public void set(int left, int right) {
        leftElement = left;
        rightElement = right;
    }

    /**
     * Checks two pairs for equality.
     *
     * @param obj object for comparison
     * @return
     * <code>true</code> if
     * <code>obj</code> is equal to this object,
     * <code>false</code> otherwise
     */
    public boolean equals(Object obj) {
        PairOfInts pair = (PairOfInts) obj;
        return leftElement == pair.getLeftElement() && rightElement == pair.getRightElement();
    }

    /**
     * Defines a natural sort order for pairs. Pairs are sorted first by the
     * left element, and then by the right element.
     *
     * @return a value less than zero, a value greater than zero, or zero if
     * this pair should be sorted before, sorted after, or is equal to
     * <code>obj</code>.
     */
    public int compareTo(PairOfInts pair) {
        int pl = pair.getLeftElement();
        int pr = pair.getRightElement();

        if (leftElement == pl) {
            if (rightElement < pr) {
                return -1;
            }
            if (rightElement > pr) {
                return 1;
            }
            return 0;
        }

        if (leftElement < pl) {
            return -1;
        }

        return 1;
    }

    @Override
    public int compare(final byte[] a, final int i1, final int j1) {

        int i1_len = 0, j1_len = 0;
        int l1 = ByteUtil.readInt(a, i1);
        int r1 = ByteUtil.readInt(a, i1 + 4);
        int l2 = ByteUtil.readInt(a, j1);
        int r2 = ByteUtil.readInt(a, j1 + 4);

        if (l1 == l2) {
            if (r1 < r2) {
                return -1;
            }
            if (r1 > r2) {
                return 1;
            }
            return 0;
        }

        if (l1 < l2) {
            return -1;
        }

        return 1;
    }

    /**
     * Returns a hash code value for the pair.
     *
     * @return hash code for the pair
     */
    public int hashCode() {
        return leftElement + rightElement;
    }

    /**
     * Generates human-readable String representation of this pair.
     *
     * @return human-readable String representation of this pair
     */
    public String toString() {
        return "(" + leftElement + ", " + rightElement + ")";
    }

    /**
     * Clones this object.
     *
     * @return clone of this object
     */
    public PairOfInts clone() {
        return new PairOfInts(this.leftElement, this.rightElement);
    }

    public PairOfInts create(byte[] bytes, int offset) throws IOException {
        PairOfInts m = new PairOfInts();
        m.readFields(bytes, offset);
        return m;
    }
//	/** Comparator optimized for <code>PairOfInts</code>. */
//	public static class Comparator extends WritableComparator {
//
//		/**
//		 * Creates a new Comparator optimized for <code>PairOfInts</code>.
//		 */
//		public Comparator() {
//			super(PairOfInts.class);
//		}
//
//		/**
//		 * Optimization hook.
//		 */
//		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//			int thisLeftValue = readInt(b1, s1);
//			int thatLeftValue = readInt(b2, s2);
//
//			if (thisLeftValue == thatLeftValue) {
//				int thisRightValue = readInt(b1, s1 + 4);
//				int thatRightValue = readInt(b2, s2 + 4);
//
//				return (thisRightValue < thatRightValue ? -1
//						: (thisRightValue == thatRightValue ? 0 : 1));
//			}
//
//			return (thisLeftValue < thatLeftValue ? -1 : (thisLeftValue == thatLeftValue ? 0 : 1));
//		}
//	}
//
//	static { // register this comparator
//		WritableComparator.define(PairOfInts.class, new Comparator());
//	}

    @Override
    public int getOffset() {
        return 0;
    }

    @Override
    public void set(WritableComparable obj) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
