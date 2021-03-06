
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class implements an output stream in which the data is written longo a
 * byte array. The buffer automatically grows as data is written to it. <p> The
 * data can be retrieved using
 * <code>toByteArray()</code> and
 * <code>toString()</code>. <p> Closing a <tt>ByteArrayOutputStream</tt> has no
 * effect. The methods in this class can be called after the stream has been
 * closed without generating an <tt>IOException</tt>. <p> This is an alternative
 * implementation of the java.io.ByteArrayOutputStream class. The original
 * implementation only allocates 32 bytes at the beginning. As this class is
 * designed for heavy duty it starts at 1024 bytes. In contrast to the original
 * it doesn't reallocate the whole memory block but allocates additional
 * buffers. This way no buffers need to be garbage collected and the contents
 * don't have to be copied to the new buffer. This class is designed to behave
 * exactly like the original. The only exception is the deprecated
 * toString(long) method that has been ignored.
 *
 * @author <a href="mailto:jeremias@apache.org">Jeremias Maerki</a>
 * @author Holger Hoffstatte
 * @version $Id: ByteArrayOutputStream.java 491007 2006-12-29 13:50:34Z
 * scolebourne $
 */
public class ByteArrayOutputStream_long extends OutputStream {

    /**
     * A singleton empty byte array.
     */
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    /**
     * The list of buffers, which grows and never reduces.
     */
    private List buffers = new ArrayList();
    /**
     * The index of the current buffer.
     */
    private int currentBufferIndex;
    /**
     * The total count of bytes in all the filled buffers.
     */
    private long filledBufferSum;
    /**
     * The current buffer.
     */
    private byte[] currentBuffer;
    /**
     * The total count of bytes written.
     */
    private long count;

    /**
     * Creates a new byte array output stream. The buffer capacity is initially
     * 1024 bytes, though its size increases if necessary.
     */
    public ByteArrayOutputStream_long() {
        this(1024);
    }

    /**
     * Creates a new byte array output stream, with a buffer capacity of the
     * specified size, in bytes.
     *
     * @param size the initial size
     * @throws IllegalArgumentException if size is negative
     */
    public ByteArrayOutputStream_long(final int size) {
        if (size < 0) {
            throw new IllegalArgumentException(
                    "Negative initial size: " + size);
        }
        needNewBuffer(size);
    }

    /**
     * Return the appropriate
     * <code>byte[]</code> buffer specified by index.
     *
     * @param index the index of the buffer required
     * @return the buffer
     */
    private byte[] getBuffer(final int index) {
        return (byte[]) buffers.get(index);
    }

    /**
     * Makes a new buffer available either by allocating a new one or re-cycling
     * an existing one.
     *
     * @param newcount the size of the buffer if one is created
     */
    private void needNewBuffer(final long newcount) {
        if (currentBufferIndex < buffers.size() - 1) {
            //Recycling old buffer
            filledBufferSum += currentBuffer.length;

            currentBufferIndex++;
            currentBuffer = getBuffer(currentBufferIndex);
        } else {
            //Creating new buffer
            long newBufferSize;
            if (currentBuffer == null) {
                newBufferSize = newcount;
                filledBufferSum = 0;
            } else {
                newBufferSize = Math.max(
                        currentBuffer.length << 1,
                        newcount - filledBufferSum);
                filledBufferSum += currentBuffer.length;
            }

            currentBufferIndex++;
            currentBuffer = new byte[(int) newBufferSize];
            buffers.add(currentBuffer);
        }
    }

    /**
     * @see java.io.OutputStream#write(byte[], long, long)
     */
    public final void write(final byte[] b, final int off, final int len) {
        if ((off < 0)
                || (off > b.length)
                || (len < 0)
                || ((off + len) > b.length)
                || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        long newcount = count + len;
        long remaining = len;
        long inBufferPos = count - filledBufferSum;
        while (remaining > 0) {
            long part = Math.min(remaining, currentBuffer.length - inBufferPos);
            System.arraycopy(b, (int) (off + len - remaining), currentBuffer, (int) inBufferPos, (int) part);
            remaining -= part;
            if (remaining > 0) {
                needNewBuffer(newcount);
                inBufferPos = 0;
            }
        }
        count = newcount;
    }

    /**
     * @see java.io.OutputStream#write(long)
     */
    public final void write(final int b) {
        long inBufferPos = count - filledBufferSum;
        if (inBufferPos == currentBuffer.length) {
            needNewBuffer(count + 1);
            inBufferPos = 0;
        }
        currentBuffer[(int) inBufferPos] = (byte) (int) b;
        count++;
    }

    /**
     * @see java.io.ByteArrayOutputStream#size()
     */
    public final long size() {
        return count;
    }

    /**
     * Closing a <tt>ByteArrayOutputStream</tt> has no effect. The methods in
     * this class can be called after the stream has been closed without
     * generating an <tt>IOException</tt>.
     *
     * @throws IOException never (this method should not declare this exception
     * but it has to now due to backwards compatability)
     */
    @Override
    public void close() throws IOException {
        //nop
    }

    /**
     * @see java.io.ByteArrayOutputStream#reset()
     */
    public final void reset() {
        count = 0;
        filledBufferSum = 0;
        currentBufferIndex = 0;
        currentBuffer = getBuffer(currentBufferIndex);
    }

    /**
     * Writes the entire contents of this byte stream to the specified output
     * stream.
     *
     * @param out the output stream to write to
     * @throws IOException if an I/O error occurs, such as if the stream is
     * closed
     * @see java.io.ByteArrayOutputStream#writeTo(OutputStream)
     */
    public final void writeTo(final OutputStream out) throws IOException {
        long remaining = count;
        for (int i = 0; i < buffers.size(); i++) {
            byte[] buf = getBuffer(i);
            long c = Math.min(buf.length, remaining);
            out.write(buf, 0, (int) c);
            remaining -= c;
            if (remaining == 0) {
                break;
            }
        }
    }

    private int getByteCount(byte[] buf) {
        int temp1 = 0, temp2 = 0, zeroCount = 0;
        for (int i = 0; i < buf.length; ++i) {
            temp2 = i;
            if (buf[temp2] != 0) {
                temp1 = temp2;
                zeroCount = 0;
            } else {
                zeroCount++;
                if (zeroCount == 10) {
                    return temp1 + 1;
                }
            }
        }

        return -1;
    }

    /**
     * Gets the curent contents of this byte stream as a byte array. The result
     * is independent of this stream.
     *
     * @return the current contents of this output stream, as a byte array
     * @see java.io.ByteArrayOutputStream#toByteArray()
     */
    public final byte[] toByteArray() {
        long remaining = count;
        if (remaining == 0) {
            return EMPTY_BYTE_ARRAY;
        }

        //System.err.println("Remaining count " + remaining);

        byte[] newbuf;
        if ((int) remaining < 0) {
            System.err.println("long integer overflow error: count " + remaining);
            remaining = 0;
            for (int i = 0; i < buffers.size(); i++) {
                if (i == buffers.size() - 1) {
                    remaining += getByteCount(getBuffer(i));
                } else {
                    remaining += getBuffer(i).length;
                }
            }
            //System.exit(-1);
        }

        newbuf = new byte[(int) remaining];

        long pos = 0;

        for (int i = 0; i < buffers.size(); i++) {
            byte[] buf = getBuffer(i);
            long c;
            if ((int) remaining > 0) {
                c = Math.min(buf.length, remaining);
                System.arraycopy(buf, 0, newbuf, (int) pos, (int) c);
                pos += c;
                remaining -= c;
                if (remaining == 0) {
                    break;
                }
            } else {
                if (i < buffers.size() - 1) {
                    c = buf.length;
                    System.arraycopy(buf, 0, newbuf, (int) pos, (int) c);
                    pos += c;
                } else {
                    c = getByteCount(buf);
                    System.arraycopy(buf, 0, newbuf, (int) pos, (int) c);
                    break;
                }
            }
        }
        return newbuf;
    }

    /**
     * Gets the curent contents of this byte stream as a string.
     *
     * @see java.io.ByteArrayOutputStream#toString()
     */
    @Override
    public final String toString() {
        return new String(toByteArray());
    }

    /**
     * Gets the curent contents of this byte stream as a string using the
     * specified encoding.
     *
     * @param enc the name of the character encoding
     * @return the string converted from the byte array
     * @throws UnsupportedEncodingException if the encoding is not supported
     * @see java.io.ByteArrayOutputStream#toString(String)
     */
    public final String toString(final String enc) throws UnsupportedEncodingException {
        return new String(toByteArray(), enc);
    }
}
