/**
 *
 */
package org.apache.hadoop.io;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.util.ByteUtil;

/**
 * @author tim
 */
public class Text implements WritableComparable {

    protected String s;
    //private static final byte[] EMPTY_BYTES = new byte[0];
    //private byte[] bytes;
    private int length;
    private Charset charset;
    private CharsetEncoder encoder;
    public static int offset  = 2;

    public Text() {
        //  bytes = EMPTY_BYTES;
    }

    public Text(Text textObj) {
        this.s = textObj.toString();
    }

    public Text(String s) {
        this.s = s;
    }
    private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY =
            new ThreadLocal<CharsetEncoder>() {

                @Override
                protected CharsetEncoder initialValue() {
                    return Charset.forName("UTF-8").newEncoder().
                            onMalformedInput(CodingErrorAction.REPORT).
                            onUnmappableCharacter(CodingErrorAction.REPORT);
                }
            };

    @Override
    public final void readFields(DataInputStream in) throws IOException {
        s = in.readUTF();
    }

    public static String readString(DataInputStream in) throws IOException {
        final int length = WritableUtils.readVInt(in);
        final byte[] bytes = new byte[length];
        in.readFully(bytes, 0, length);
        return decode(bytes);
    }

    @Override
    public final int write(DataOutputStream out) throws IOException {
        //System.out.println(s);
        out.writeUTF(s);
        return 2 + s.length();
    }

    @Override
    public final int write(ByteBuffer buf) {
        byte b = 0;
        buf.put(b);
        b = (byte) s.length();
        buf.put(b);
        CharBuffer cbuf = buf.asCharBuffer();
        cbuf.put(s);
        cbuf.flip();
        charset = Charset.forName("UTF-8");
        encoder = charset.newEncoder();
        ByteBuffer buf1 = null;
        try {
            buf1 = encoder.encode(cbuf);
        } catch (CharacterCodingException ex) {
            Logger.getLogger(Text.class.getName()).log(Level.SEVERE, null, ex);
        }
        buf.put(buf1);
        return 2 + s.length();
    }

    public final int write(DynamicDirectByteBuffer buf) {
        try {
            buf.putString(s);
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(Text.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 2 + s.length();
    }

    @Override
    public final int getOffset() {
        return 2;
    }

    public static int writeString(DataOutputStream out, String s) throws IOException {
        final ByteBuffer bytes = encode(s);
        final int length = bytes.limit();
        WritableUtils.writeVInt(out, length);
        out.write(bytes.array(), 0, length);
        return length;
    }

    @Override
    public final String toString() {
        return s;
    }

    @Override
    public final boolean equals(Object o) {
        if (o instanceof Text
                && (s != null)) {
            return s.equals(((Text) o).toString());
        } else if (o instanceof Text
                && s == null && ((Text) o).toString() == null) {
            return true;
        }
        return false;
    }

    @Override
    public final int hashCode() {
        int hash = 7;
        hash = 59 * hash + (this.s != null ? this.s.hashCode() : 0);
        hash = 59 * hash + this.length;
        return hash;
    }

    @Override
    public final int compareTo(Object o) {
        return s.compareTo(o.toString());
    }

    public final byte[] getBytes() {
        return s.getBytes();
    }

    public final int getLength() {
        return s.length();
    }

    public final void set(String string) {
        this.s = string;
    }

    public final void set(WritableComparable text) {
        this.s = text.toString();
    }

    /**
     * Converts the provided String to bytes using the UTF-8 encoding. If the
     * input is malformed, invalid chars are replaced by a default value.
     *
     * @return ByteBuffer: bytes stores at ByteBuffer.array() and length is
     * ByteBuffer.limit()
     */
    public final static ByteBuffer encode(String string)
            throws CharacterCodingException {
        return encode(string, true);
    }

    /**
     * Converts the provided String to bytes using the UTF-8 encoding. If
     * <code>replace</code> is true, then malformed input is replaced with the
     * substitution character, which is U+FFFD. Otherwise the method throws a
     * MalformedInputException.
     *
     * @return ByteBuffer: bytes stores at ByteBuffer.array() and length is
     * ByteBuffer.limit()
     */
    public final static ByteBuffer encode(String string, boolean replace)
            throws CharacterCodingException {
        CharsetEncoder encoder = ENCODER_FACTORY.get();
        if (replace) {
            encoder.onMalformedInput(CodingErrorAction.REPLACE);
            encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
        ByteBuffer bytes =
                encoder.encode(CharBuffer.wrap(string.toCharArray()));
        if (replace) {
            encoder.onMalformedInput(CodingErrorAction.REPORT);
            encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        }
        return bytes;
    }

    /**
     * Converts the provided byte array to a String using the UTF-8 encoding. If
     * the input is malformed, replace by a default value.
     */
    public static String decode(byte[] utf8) throws CharacterCodingException {
        return decode(ByteBuffer.wrap(utf8), true);
    }

    public static String decode(byte[] utf8, int start, int length)
            throws CharacterCodingException {
        return decode(ByteBuffer.wrap(utf8, start, length), true);
    }

    /**
     * Converts the provided byte array to a String using the UTF-8 encoding. If
     * <code>replace</code> is true, then malformed input is replaced with the
     * substitution character, which is U+FFFD. Otherwise the method throws a
     * MalformedInputException.
     */
    public static String decode(byte[] utf8, int start, int length, boolean replace)
            throws CharacterCodingException {
        return decode(ByteBuffer.wrap(utf8, start, length), replace);
    }

    private static String decode(ByteBuffer utf8, boolean replace)
            throws CharacterCodingException {
        CharsetDecoder decoder = DECODER_FACTORY.get();
        if (replace) {
            decoder.onMalformedInput(
                    java.nio.charset.CodingErrorAction.REPLACE);
            decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
        String str = decoder.decode(utf8).toString();
        // set decoder back to its default value: REPORT
        if (replace) {
            decoder.onMalformedInput(CodingErrorAction.REPORT);
            decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        }
        return str;
    }
    private static ThreadLocal<CharsetDecoder> DECODER_FACTORY =
            new ThreadLocal<CharsetDecoder>() {

                @Override
                protected CharsetDecoder initialValue() {
                    return Charset.forName("UTF-8").newDecoder().
                            onMalformedInput(CodingErrorAction.REPORT).
                            onUnmappableCharacter(CodingErrorAction.REPORT);
                }
            };

    @Override
    public int compare(final byte[] a, final int i1, final int j1) {

        int i1_len = 0, j1_len = 0;

        i1_len = a[i1 - 1] & 0x000000FF;
        j1_len = a[j1 - 1] & 0x000000FF;

        int ind1 = i1;
        int ind2 = j1;

        final int n = Math.min(i1_len, j1_len);
        int delta;
        for (int i = 0; i < n; ++i) {
            delta = a[ind1++] - a[ind2++];  // OK since bytes are smaller than ints.
            if (delta != 0) {
                return delta;
            }
        }

        delta = i1_len - j1_len;

        if (delta < 0) {
            return -1;
        } else if (delta != 0) {
            return 1;
        } else {
            return 0;
        }

        //return delta < 0 ? -1 : delta != 0 ? 1 : 0;
    }

    /**
     * Deserializes this object.
     *
     * @param in source for raw byte representation
     */
    public void readFields(byte[] a, int offset) throws IOException {
        this.s = ByteUtil.readString(a, offset + getOffset(), a[offset + getOffset() - 1] & ByteUtil.MASK);
    }

    /**
     * Creates object from a
     * <code>DataInput</code>.
     *
     * @param in source for reading the serialized representation
     * @return newly-created object
     * @throws IOException
     */
    public static Text create(DataInputStream in) throws IOException {
        Text m = new Text();
        m.readFields(in);

        return m;
    }

    /**
     * Creates object from a byte array.
     *
     * @param bytes raw serialized representation
     * @return newly-created object
     * @throws IOException
     */
    public static Text create(byte[] bytes) throws IOException {
        return create(new DataInputStream(new ByteArrayInputStream(bytes)));
    }

    public Text create(byte[] bytes, int offset) throws IOException {
        Text m = new Text();
        m.readFields(bytes, offset);
        return m;
    }
}
