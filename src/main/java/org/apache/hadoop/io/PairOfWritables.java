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
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class representing pair of Writables.
 */
public class PairOfWritables<L extends WritableComparable, R extends WritableComparable> implements WritableComparable<PairOfWritables> {

    private L leftElement;
    private R rightElement;

    /**
     * Creates a new
     * <code>PairOfWritables</code>.
     */
    public PairOfWritables() {
    }

    /**
     * Creates a new
     * <code>PairOfWritables</code>.
     */
    public PairOfWritables(L left, R right) {
        leftElement = left;
        rightElement = right;
    }

    /**
     * Deserializes the pair.
     *
     * @param in source for raw byte representation
     */
    public void readFields(DataInputStream in) throws IOException {
        String keyClassName = in.readUTF();
        String valueClassName = in.readUTF();

        try {
            Class<L> keyClass = (Class<L>) Class.forName(keyClassName);
            leftElement = (L) keyClass.newInstance();
            Class<R> valueClass = (Class<R>) Class.forName(valueClassName);
            rightElement = (R) valueClass.newInstance();

            leftElement.readFields(in);
            rightElement.readFields(in);
        } catch (Exception e) {
            throw new RuntimeException("Unable to create PairOfWritables!");
        }
    }

    public void readFields(byte[] a, int offset) throws IOException {

        try {
            Class<L> key = null;
            Class<R> value = null;
            L keyClass = key.newInstance();
            leftElement = (L) keyClass.create(a, offset);
            R valueClass = value.newInstance();
            rightElement = (R) valueClass.create(a, offset);

        } catch (Exception e) {
            throw new RuntimeException("Unable to create PairOfWritables!");
        }
    }

    public PairOfWritables create(byte[] bytes, int offset) throws IOException {
        PairOfWritables m = new PairOfWritables();
        m.readFields(bytes, offset);
        return m;
    }

    /**
     * Serializes this pair.
     *
     * @param out where to write the raw byte representation
     */
    public int write(DataOutputStream out) throws IOException {
        out.writeUTF(leftElement.getClass().getCanonicalName());
        out.writeUTF(rightElement.getClass().getCanonicalName());

        leftElement.write(out);
        rightElement.write(out);

        return 0;
    }

    /**
     * Returns the left element.
     *
     * @return the left element
     */
    public L getLeftElement() {
        return leftElement;
    }

    /**
     * Returns the right element.
     *
     * @return the right element
     */
    public R getRightElement() {
        return rightElement;
    }

    /**
     * Returns the key (left element).
     *
     * @return the key
     */
    public L getKey() {
        return leftElement;
    }

    /**
     * Returns the value (right element).
     *
     * @return the value
     */
    public R getValue() {
        return rightElement;
    }

    /**
     * Sets the right and left elements of this pair.
     *
     * @param left the left element
     * @param right the right element
     */
    public void set(L left, R right) {
        leftElement = left;
        rightElement = right;
    }

    /**
     * Generates human-readable String representation of this pair.
     *
     * @return human-readable String representation of this pair
     */
    public String toString() {
        return "(" + leftElement + ", " + rightElement + ")";
    }

    @Override
    public int compare(byte[] a, int i1, int j1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getOffset() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int compareTo(PairOfWritables o) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void set(WritableComparable obj) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int write(ByteBuffer buf) {
        byte b = 0;
        buf.put(b);
        b = (byte) leftElement.getClass().getCanonicalName().length();
        buf.put(b);
        CharBuffer cbuf = buf.asCharBuffer();
        cbuf.put(leftElement.getClass().getCanonicalName());
        b = (byte) rightElement.getClass().getCanonicalName().length();
        cbuf.put(rightElement.getClass().getCanonicalName());
        Charset charset = Charset.forName("UTF-8");
        CharsetEncoder encoder = charset.newEncoder();
        cbuf.flip();
        ByteBuffer buf1 = null;
        try {
            buf1 = encoder.encode(cbuf);
        } catch (CharacterCodingException ex) {
            Logger.getLogger(PairOfWritables.class.getName()).log(Level.SEVERE, null, ex);
        }
        buf.put(buf1);
        return 0;
    }

    @Override
    public int write(DynamicDirectByteBuffer buf) {
        try {
            buf.putString(leftElement.getClass().getCanonicalName());
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(PairOfWritables.class.getName()).log(Level.SEVERE, null, ex);
        }
        //b = (byte) rightElement.getClass().getCanonicalName().length();
        buf.putStringNoSep(rightElement.getClass().getCanonicalName());
        return 0;
    }
}
