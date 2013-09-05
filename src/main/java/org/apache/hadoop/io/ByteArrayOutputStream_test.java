/*
 * Copyright (c) 1994, 2010, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */
package org.apache.hadoop.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * This class implements an output stream in which the data is written into a
 * byte array. The buffer automatically grows as data is written to it. The data
 * can be retrieved using
 * <code>toByteArray()</code> and
 * <code>toString()</code>. <p> Closing a <tt>ByteArrayOutputStream_test</tt>
 * has no effect. The methods in this class can be called after the stream has
 * been closed without generating an <tt>IOException</tt>.
 *
 * @author Arthur van Hoff
 * @since JDK1.0
 */
public class ByteArrayOutputStream_test extends OutputStream {

    /**
     * The buffer where data is stored.
     */
    protected byte[] buf;
    /**
     * The number of valid bytes in the buffer.
     */
    protected int count;
    protected int BLOCK_SIZE = 32;
    private static final int CAPACITY_INCREMENT_NUM = 3;	//numerator of the increment factor
    private static final int CAPACITY_INCREMENT_DEN = 2;	//denominator of the increment factor

    /**
     * Creates a new byte array output stream. The buffer capacity is initially
     * 32 bytes, though its size increases if necessary.
     */
    public ByteArrayOutputStream_test() {
        this(1024);
    }

    /**
     * Creates a new byte array output stream, with a buffer capacity of the
     * specified size, in bytes.
     *
     * @param size the initial size.
     * @exception IllegalArgumentException if size is negative.
     */
    public ByteArrayOutputStream_test(final int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Negative initial size: "
                    + size);
        }
        buf = new byte[size];
    }

    /**
     * Increases the capacity if necessary to ensure that it can hold at least
     * the number of elements specified by the minimum capacity argument.
     *
     * @param minCapacity the desired minimum capacity
     * @throws OutOfMemoryError if {@code minCapacity < 0}.  This is
     * interpreted as a request for the unsatisfiably large capacity
     * {@code (long) Integer.MAX_VALUE + (minCapacity - Integer.MAX_VALUE)}.
     */
    private void ensureCapacity(final int minCapacity) {
        // overflow-conscious code
        if (minCapacity - buf.length > 0) {
            grow(minCapacity);
        }
    }
    /**
     * The maximum size of array to allocate. Some VMs reserve some header words
     * in an array. Attempts to allocate larger arrays may result in
     * OutOfMemoryError: Requested array size exceeds VM limit
     */
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    /**
     * Increases the capacity to ensure that it can hold at least the number of
     * elements specified by the minimum capacity argument.
     *
     * @param minCapacity the desired minimum capacity
     */
    private void grow(final int minCapacity) {
        // overflow-conscious code
        final int oldCapacity = buf.length;
        //int newCapacity = oldCapacity << 1;
        //int newCapacity = oldCapacity + (oldCapacity >> 1);

        int newCapacity = ((oldCapacity < 64)
                ? ((oldCapacity + 1) * 2)
                : ((oldCapacity / 2) * 3));

//        if (newCapacity < 0) // overflow
//        {
//            newCapacity = Integer.MAX_VALUE;
//        }
        //int newCapacity = oldCapacity;

        if (newCapacity - minCapacity < 0) {
            newCapacity = minCapacity;
        }

        if (newCapacity < 0) {
            if (minCapacity < 0) // overflow
            {
                throw new OutOfMemoryError();
            }
            newCapacity = Integer.MAX_VALUE;
        }

        if (newCapacity - MAX_ARRAY_SIZE > 0) {
            newCapacity = hugeCapacity(minCapacity);
        }
        // minCapacity is usually close to size, so this is a win:
        buf = Arrays.copyOf(buf, newCapacity);
    }

    private int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
        {
            throw new OutOfMemoryError();
        }
        return (minCapacity > MAX_ARRAY_SIZE)
                ? Integer.MAX_VALUE
                : MAX_ARRAY_SIZE;
    }

    /**
     * Writes the specified byte to this byte array output stream.
     *
     * @param b the byte to be written.
     */
    @Override
    public void write(final int b) {
        ensureCapacity(count + 1);
        buf[count] = (byte) b;
        count += 1;
    }

    /**
     * Writes
     * <code>len</code> bytes from the specified byte array starting at offset
     * <code>off</code> to this byte array output stream.
     *
     * @param b the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     */
    @Override
    public final void write(final byte[] b, final int off, final int len) {
        if ((off < 0) || (off > b.length) || (len < 0)
                || ((off + len) - b.length > 0)) {
            throw new IndexOutOfBoundsException();
        }
        ensureCapacity(count + len);
        System.arraycopy(b, off, buf, count, len);
        count += len;
    }

    /**
     * Writes the complete contents of this byte array output stream to the
     * specified output stream argument, as if by calling the output stream's
     * write method using
     * <code>out.write(buf, 0, count)</code>.
     *
     * @param out the output stream to which to write the data.
     * @exception IOException if an I/O error occurs.
     */
    public final void writeTo(final OutputStream out) throws IOException {
        out.write(buf, 0, count);
    }

    /**
     * Resets the
     * <code>count</code> field of this byte array output stream to zero, so
     * that all currently accumulated output in the output stream is discarded.
     * The output stream can be used again, reusing the already allocated buffer
     * space.
     *
     * @see java.io.ByteArrayInputStream#count
     */
    public final void reset() {
        count = 0;
    }

    public byte[] getBuf() {
        return buf;
    }

    /**
     * Creates a newly allocated byte array. Its size is the current size of
     * this output stream and the valid contents of the buffer have been copied
     * into it.
     *
     * @return the current contents of this output stream, as a byte array.
     * @see java.io.ByteArrayOutputStream_test#size()
     */
    public final byte[] toByteArray() {
        return Arrays.copyOf(buf, count);
        //return buf;
    }

    /**
     * Returns the current size of the buffer.
     *
     * @return the value of the
     * <code>count</code> field, which is the number of valid bytes in this
     * output stream.
     * @see java.io.ByteArrayOutputStream_test#count
     */
    public final int size() {
        return count;
    }

    /**
     * Converts the buffer's contents into a string decoding bytes using the
     * platform's default character set. The length of the new <tt>String</tt>
     * is a function of the character set, and hence may not be equal to the
     * size of the buffer.
     *
     * <p> This method always replaces malformed-input and unmappable-character
     * sequences with the default replacement string for the platform's default
     * character set. The {@linkplain java.nio.charset.CharsetDecoder} class
     * should be used when more control over the decoding process is required.
     *
     * @return String decoded from the buffer's contents.
     * @since JDK1.1
     */
    @Override
    public final String toString() {
        return new String(buf, 0, count);
    }

    /**
     * Converts the buffer's contents into a string by decoding the bytes using
     * the specified {@link java.nio.charset.Charset charsetName}. The length of
     * the new <tt>String</tt> is a function of the charset, and hence may not
     * be equal to the length of the byte array.
     *
     * <p> This method always replaces malformed-input and unmappable-character
     * sequences with this charset's default replacement string. The {@link
     * java.nio.charset.CharsetDecoder} class should be used when more control
     * over the decoding process is required.
     *
     * @param charsetName the name of a supported
     *              {@linkplain java.nio.charset.Charset </code>charset<code>}
     * @return String decoded from the buffer's contents.
     * @exception UnsupportedEncodingException If the named charset is not
     * supported
     * @since JDK1.1
     */
    public final String toString(final String charsetName)
            throws UnsupportedEncodingException {
        return new String(buf, 0, count, charsetName);
    }

    /**
     * Creates a newly allocated string. Its size is the current size of the
     * output stream and the valid contents of the buffer have been copied into
     * it. Each character <i>c</i> in the resulting string is constructed from
     * the corresponding element <i>b</i> in the byte array such that: <blockquote><pre>
     *     c == (char)(((hibyte &amp; 0xff) &lt;&lt; 8) | (b &amp; 0xff))
     * </pre></blockquote>
     *
     * @deprecated This method does not properly convert bytes into characters.
     * As of JDK&nbsp;1.1, the preferred way to do this is via the
     * <code>toString(String enc)</code> method, which takes an encoding-name
     * argument, or the
     * <code>toString()</code> method, which uses the platform's default
     * character encoding.
     *
     * @param hibyte the high byte of each resulting Unicode character.
     * @return the current contents of the output stream, as a string.
     * @see java.io.ByteArrayOutputStream_test#size()
     * @see java.io.ByteArrayOutputStream_test#toString(String)
     * @see java.io.ByteArrayOutputStream_test#toString()
     */
    @Deprecated
    public final String toString(final int hibyte) {
        return new String(buf, hibyte, 0, count);
    }

    /**
     * Closing a <tt>ByteArrayOutputStream_test</tt> has no effect. The methods
     * in this class can be called after the stream has been closed without
     * generating an <tt>IOException</tt>. <p>
     *
     */
    @Override
    public void close() throws IOException {
    }
}
