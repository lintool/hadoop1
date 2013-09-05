/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.io;

import java.io.UnsupportedEncodingException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.util.DirectByteBufferCleaner;

/**
 *
 * @author ashwinkayyoor
 */
public class DynamicDirectByteBuffer {

    static ByteBuffer[] byteBufferChain;
    int[] count;
    int currentIndex = 0;
    int readIndex = 0;
    int minCapacity;

    public DynamicDirectByteBuffer(int minCapacity) {
        byteBufferChain = new ByteBuffer[500000];
        count = new int[500000];
        byteBufferChain[0] = ByteBuffer.allocateDirect(minCapacity);
        this.minCapacity = minCapacity;
    }

    public void clear() {
        for (int i = 0; i <= currentIndex; ++i) {
            byteBufferChain[i].clear();
        }
    }

    public void destroy() {
        for (int i = 0; i <= currentIndex; ++i) {
            DirectByteBufferCleaner.clean(byteBufferChain[i]);
            byteBufferChain[i] = null;
        }
    }

    public Buffer flip() {
        return byteBufferChain[currentIndex].flip();
    }

    public void grow() {
        currentIndex++;
        byteBufferChain[currentIndex] = ByteBuffer.allocateDirect(minCapacity);
    }

    public byte get() {
        if (byteBufferChain[readIndex].position() == count[readIndex]) {
            readIndex++;
            //byteBufferChain[readIndex].clear();
        }
        return byteBufferChain[readIndex].get();
    }

    public byte[] get(byte[] dst) {
        byte[] a, b = null;
        a = new byte[count[0]];
        byteBufferChain[0].get(a);
        if (currentIndex + 1 == 1) {
            dst = a;
        } else if (currentIndex + 1 > 1) {
            b = new byte[count[1]];
            byteBufferChain[1].get(b);
        }
        byte[] temp = null;
        if (currentIndex + 1 == 2) {
            temp = ArrayUtils.addAll(a, b);
        } else if (currentIndex + 1 > 2) {
            for (int i = 2; i <= currentIndex; ++i) {
                temp = ArrayUtils.addAll(a, b);
                a = temp;
                b = new byte[count[i]];
                byteBufferChain[i].get(b);
            }
            temp = ArrayUtils.addAll(a, b);
        }

        if (temp != null) {
            return temp.clone();
        } else {
            return dst;
        }
    }

    //This method is broken
    public ByteBuffer get(byte[] dst, int offset, int length) {
        if (byteBufferChain[readIndex].position() == count[readIndex]) {
            readIndex++;
            //byteBufferChain[readIndex].clear();
        }
        return byteBufferChain[readIndex].get(dst, offset, length);
    }

    public byte get(int index) {
        if (byteBufferChain[readIndex].position() == count[readIndex]) {
            readIndex++;
            //byteBufferChain[readIndex].clear();
        }
        return byteBufferChain[readIndex].get(index);
    }

    public int position() {
        int sum = 0;
        for (int i = 0; i <= currentIndex; ++i) {
            sum += count[i];
        }
        return sum;
    }

    public ByteBuffer put(byte b) {
        if (byteBufferChain[currentIndex].limit() < byteBufferChain[currentIndex].position() + 1) {
            grow();
        }
        byteBufferChain[currentIndex].put(b);
        count[currentIndex] = byteBufferChain[currentIndex].position();
        return byteBufferChain[currentIndex];
    }

    public ByteBuffer put(ByteBuffer src) {
        if (byteBufferChain[currentIndex].limit() < byteBufferChain[currentIndex].position() + src.limit()) {
            grow();
        }
        byteBufferChain[currentIndex].put(src);
        count[currentIndex] = byteBufferChain[currentIndex].position();
        return byteBufferChain[currentIndex];
    }

    public ByteBuffer putInt(int i) {
        if (byteBufferChain[currentIndex].limit() < byteBufferChain[currentIndex].position() + 4) {
            grow();
        }
        byteBufferChain[currentIndex].putInt(i);
        count[currentIndex] = byteBufferChain[currentIndex].position();
        return byteBufferChain[currentIndex];
    }

    public ByteBuffer putFloat(float f) {
        if (byteBufferChain[currentIndex].limit() < byteBufferChain[currentIndex].position() + 4) {
            grow();
        }
        byteBufferChain[currentIndex].putFloat(f);
        count[currentIndex] = byteBufferChain[currentIndex].position();
        return byteBufferChain[currentIndex];
    }

    public ByteBuffer putDouble(double d) {
        if (byteBufferChain[currentIndex].limit() < byteBufferChain[currentIndex].position() + 8) {
            grow();
        }
        byteBufferChain[currentIndex].putDouble(d);
        count[currentIndex] = byteBufferChain[currentIndex].position();
        return byteBufferChain[currentIndex];
    }

    public ByteBuffer putChar(char c) {
        if (byteBufferChain[currentIndex].limit() < byteBufferChain[currentIndex].position() + 2) {
            grow();
        }
        byteBufferChain[currentIndex].putChar(c);
        count[currentIndex] = byteBufferChain[currentIndex].position();
        return byteBufferChain[currentIndex];
    }

    public ByteBuffer putLong(long l) {
        if (byteBufferChain[currentIndex].limit() < byteBufferChain[currentIndex].position() + 8) {
            grow();
        }
        byteBufferChain[currentIndex].putLong(l);
        count[currentIndex] = byteBufferChain[currentIndex].position();
        return byteBufferChain[currentIndex];
    }

    public ByteBuffer putString(String s) throws UnsupportedEncodingException {
        if (byteBufferChain[currentIndex].limit() < byteBufferChain[currentIndex].position() + 2 + s.length()) {
            grow();
        }

        byte b = 0;
        byteBufferChain[currentIndex].put(b);
        b = (byte) s.length();
        byteBufferChain[currentIndex].put(b);
        byte[] sbytes = s.getBytes("UTF-8");
        for (int i = 0; i < sbytes.length; ++i) {
            byteBufferChain[currentIndex].put(sbytes[i]);
        }
//        CharBuffer cbuf = byteBufferChain[currentIndex].asCharBuffer();
//        //System.out.println("cbuf limit: " + cbuf.limit() + " " + cbuf.isDirect());
//        cbuf.put(s);
//        cbuf.flip();
//        Charset charset = Charset.forName("UTF-8");
//        CharsetEncoder encoder = charset.newEncoder();
//        ByteBuffer buf1 = null;
//        try {
//            buf1 = encoder.encode(cbuf);
//
//
//        } catch (CharacterCodingException ex) {
//            Logger.getLogger(Text.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        byteBufferChain[currentIndex].put(buf1);
        count[currentIndex] = byteBufferChain[currentIndex].position();
        return byteBufferChain[currentIndex];
    }

    public ByteBuffer putStringNoSep(String s) {
        if (byteBufferChain[currentIndex].limit() < byteBufferChain[currentIndex].position() + 2 + s.length()) {
            grow();
        }

//        byte b = 0;
//        byteBufferChain[currentIndex].put(b);
        byte b = (byte) s.length();
        byteBufferChain[currentIndex].put(b);
        CharBuffer cbuf = byteBufferChain[currentIndex].asCharBuffer();
        //System.out.println("cbuf limit: " + cbuf.limit() + " " + cbuf.isDirect());
        cbuf.put(s);
        cbuf.flip();
        Charset charset = Charset.forName("UTF-8");
        CharsetEncoder encoder = charset.newEncoder();
        ByteBuffer buf1 = null;
        try {
            buf1 = encoder.encode(cbuf);


        } catch (CharacterCodingException ex) {
            Logger.getLogger(Text.class.getName()).log(Level.SEVERE, null, ex);
        }
        byteBufferChain[currentIndex].put(buf1);
        count[currentIndex] = byteBufferChain[currentIndex].position();
        return byteBufferChain[currentIndex];
    }
}
