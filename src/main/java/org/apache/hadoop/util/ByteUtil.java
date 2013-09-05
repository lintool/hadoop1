/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

/**
 *
 * @author ashwinkayyoor
 */
public class ByteUtil {

    private final static int INT_64 = 64, INT_8 = 8;
    private final static int MASK = 0x000000FF;    

    public static void writeInt(byte[] b, int i, int value) {
        b[i++] = (byte) (value >> 24);
        b[i++] = (byte) (value >> 16);
        b[i++] = (byte) (value >> 8);
        b[i++] = (byte) (value);
    }

    public static int readInt(final byte[] a, final int i) {
        //return ByteBuffer.wrap(Arrays.copyOfRange(a, i, j)).getInt();
        return a[i] << 24 | (a[i + 1] & 0x000000FF) << 16 | (a[i + 2] & 0x000000FF) << 8 | (a[i + 3] & 0x000000FF);
    }

    public static float readFloat(final byte[] b, final int offset) {
        if (b.length != 4) {
            return 0.0F;
        }

        final int i = readInt(b, offset);

        return Float.intBitsToFloat(i);
    }

    public static double readDouble(final byte[] b, final int offset) {
        long accum = 0;
        int i = offset;
        for (int shiftBy = 0; shiftBy < INT_64; shiftBy += INT_8) {
            accum |= ((long) (b[i] & 0xff)) << shiftBy;
            i++;
        }

        return Double.longBitsToDouble(accum);
    }

    public static double toDouble(final byte[] b, final int offset) {
        byte[] bytes = new byte[8];
        int ind = 0;
        for (int i = offset; i < offset + 8; ++i) {
            bytes[ind++] = b[i];
        }
        return ByteBuffer.wrap(bytes).getDouble();
    }

    private static String decodeUTF16BE(final byte[] bytes, int startByte, final int byteCount) {
        final StringBuilder builder = new StringBuilder(byteCount);
        //char[] buffer = new char[byteCount];
        //builder.delete(0, builder.length());
        //int sByte = startByte;

        for (int i = 0; i < byteCount; i++) {
            final byte low = bytes[startByte++];
            //byte high = bytes[startByte++];
            final int ch = (low & MASK);
            builder.append((char) ch);
            //buffer[i] = (char) ch;
        }

        return builder.toString();
        //return new String(buffer);
    }

    public static String readString(final byte[] a, final int i, final int j) throws CharacterCodingException {
        //return new String(Arrays.copyOfRange(a, i, j + 1));
        //byte[] bytes = Arrays.copyOfRange(a, i, j+1);
        //return Text.decode(bytes);
        return decodeUTF16BE(a, i, (j - i) + 1);
    }
}
