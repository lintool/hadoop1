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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.util.ByteUtil;

/**
 * WritableComparable representing a pair of an int and long. The elements in
 * the pair are referred to as the left and right elements. The natural sort
 * order is: first by the left element, and then by the right element.
 *
 * @author Jimmy Lin
 */
public class PairOfIntFloat implements WritableComparable<PairOfIntFloat> {

    private int leftElement;
    private float rightElement;

    /**
     * Creates a pair.
     */
    public PairOfIntFloat() {
    }

    /**
     * Creates a pair.
     *
     * @param left the left element
     * @param right the right element
     */
    public PairOfIntFloat(int left, float right) {
        set(left, right);
    }

    /**
     * Deserializes this pair.
     *
     * @param in source for raw byte representation
     */
    public final void readFields(DataInputStream in) throws IOException {
        leftElement = in.readInt();
        rightElement = in.readFloat();
    }

    /**
     * Serializes this pair.
     *
     * @param out where to write the raw byte representation
     */
    public final int write(DataOutputStream out) throws IOException {
        out.writeInt(leftElement);
        out.writeFloat(rightElement);
        return 8;
    }

    @Override
    public final int write(ByteBuffer buf) {
        buf.putInt(leftElement);
        buf.putFloat(rightElement);
        return 8;
    }

    @Override
    public final int write(DynamicDirectByteBuffer buf) {
        buf.putInt(leftElement);
        buf.putFloat(rightElement);
        return 8;
    }

    /**
     * Returns the left element.
     *
     * @return the left element
     */
    public final int getLeftElement() {
        return leftElement;
    }

    /**
     * Returns the right element.
     *
     * @return the right element
     */
    public final float getRightElement() {
        return rightElement;
    }

    /**
     * Returns the key (left element).
     *
     * @return the key
     */
    public final int getKey() {
        return leftElement;
    }

    /**
     * Returns the value (right element).
     *
     * @return the value
     */
    public final float getValue() {
        return rightElement;
    }

    /**
     * Sets the right and left elements of this pair.
     *
     * @param left the left element
     * @param right the right element
     */
    public final void set(final int left, final float right) {
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
    public final boolean equals(final Object obj) {
        PairOfIntFloat pair = (PairOfIntFloat) obj;
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
    @Override
    public final int compareTo(final PairOfIntFloat pair) {
        int pl = pair.getLeftElement();
        float pr = pair.getRightElement();

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

    /**
     * Returns a hash code value for the pair.
     *
     * @return hash code for the pair
     */
    @Override
    public final int hashCode() {
        return (int) (leftElement + rightElement);
    }

    /**
     * Generates human-readable String representation of this pair.
     *
     * @return human-readable String representation of this pair
     */
    @Override
    public final String toString() {
        return "(" + leftElement + ", " + rightElement + ")";
    }

    /**
     * Clones this object.
     *
     * @return clone of this object
     */
    public final PairOfIntFloat clone() {
        return new PairOfIntFloat(this.leftElement, this.rightElement);
    }

    public final int readInt(final byte[] a, final int i) {
        //return ByteBuffer.wrap(Arrays.copyOfRange(a, i, j)).getInt();
        return a[i] << 24 | (a[i + 1] & 0x000000FF) << 16 | (a[i + 2] & 0x000000FF) << 8 | (a[i + 3] & 0x000000FF);
    }

    public final float readFloat(final byte[] b, final int offset) {
        if (b.length != 4) {
            return 0.0F;
        }

        int i = readInt(b, offset);

        return Float.intBitsToFloat(i);
    }

    public final int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int thisLeftValue = readInt(b1, s1);
        int thatLeftValue = readInt(b2, s2);

        if (thisLeftValue == thatLeftValue) {
            float thisRightValue = readFloat(b1, s1 + 4);
            float thatRightValue = readFloat(b2, s2 + 4);

            return (thisRightValue < thatRightValue ? -1
                    : (thisRightValue == thatRightValue ? 0 : 1));
        }

        return (thisLeftValue < thatLeftValue ? -1 : (thisLeftValue == thatLeftValue ? 0 : 1));
    }

    @Override
    public int compare(final byte[] a, final int i1, final int j1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public final int getOffset() {
        return 0;
    }

    @Override
    public void set(WritableComparable obj) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PairOfIntFloat create(byte[] input, int offset) throws IOException {
        PairOfIntFloat m = new PairOfIntFloat();
        m.readFields(input, offset);
        return m;
    }

    @Override
    public void readFields(byte[] input, int offset) throws IOException {
        leftElement = ByteUtil.readInt(input, offset);
        rightElement = ByteUtil.readFloat(input, offset);
    }
}