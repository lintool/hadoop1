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
package org.apache.hadoop.util.map;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.DynamicDirectByteBuffer;
import org.apache.hadoop.io.WritableComparable;

/**
 * Writable representing a map where keys are Strings and values are ints. This
 * class is specialized for String objects to avoid the overhead that comes with
 * wrapping Strings inside
 * <code>Text</code> objects.
 *
 * @author Jimmy Lin
 */
public class HMapSIW extends HMapKI<String> implements WritableComparable {

    private static final long serialVersionUID = -9179978557431493856L;

    /**
     * Creates a
     * <code>HMapSIW</code> object.
     */
    public HMapSIW() {
        super();
    }

    /**
     * Deserializes the map.
     *
     * @param in source for raw byte representation
     */
    @Override
    public void readFields(DataInputStream in) throws IOException {
        this.clear();

        int numEntries = in.readInt();
        if (numEntries == 0) {
            return;
        }

        for (int i = 0; i < numEntries; i++) {
            String k = in.readUTF();
            int v = in.readInt();
            put(k, v);
        }
    }

    /**
     * Serializes the map.
     *
     * @param out where to write the raw byte representation
     */
    @Override
    public int write(DataOutputStream out) throws IOException {
        // Write out the number of entries in the map.
        int bytesWritten = 0;
        out.writeInt(size());
        bytesWritten += 4;
        if (size() == 0) {
            return bytesWritten;
        }

        // Then write out each key/value pair.
        for (MapKI.Entry<String> e : entrySet()) {
            out.writeUTF(e.getKey());
            bytesWritten += (1 + e.getKey().length());
            out.writeInt(e.getValue());
            bytesWritten += 4;
        }

        return bytesWritten;
    }

    @Override
    public int write(ByteBuffer buf){
        // Write out the number of entries in the map.
        int bytesWritten = 0;
        buf.putInt(size());
        bytesWritten += 4;
        if (size() == 0) {
            return bytesWritten;
        }

        // Then write out each key/value pair.
        for (MapKI.Entry<String> e : entrySet()) {
            byte b = 0;
            buf.put(b);
            b = (byte) e.getKey().length();
            buf.put(b);
            CharBuffer cbuf = buf.asCharBuffer();
            cbuf.put(e.getKey());
            Charset charset = Charset.forName("UTF-8");
            CharsetEncoder encoder = charset.newEncoder();
            cbuf.flip();
            ByteBuffer buf1 = null;
            try {
                buf1 = encoder.encode(cbuf);
            } catch (CharacterCodingException ex) {
                Logger.getLogger(HMapSIW.class.getName()).log(Level.SEVERE, null, ex);
            }
            buf.put(buf1);

            bytesWritten += (1 + e.getKey().length());
            buf.putInt(e.getValue());
            bytesWritten += 4;
        }

        return bytesWritten;
    }

    @Override
    public int write(DynamicDirectByteBuffer buf){
        // Write out the number of entries in the map.
        int bytesWritten = 0;
        buf.putInt(size());
        bytesWritten += 4;
        if (size() == 0) {
            return bytesWritten;
        }

        // Then write out each key/value pair.
        for (MapKI.Entry<String> e : entrySet()) {
            try {
                buf.putString(e.getKey());
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(HMapSIW.class.getName()).log(Level.SEVERE, null, ex);
            }
            bytesWritten += (1 + e.getKey().length());
            buf.putInt(e.getValue());
            bytesWritten += 4;
        }

        return bytesWritten;
    }

    /**
     * Returns the serialized representation of this object as a byte array.
     *
     * @return byte array representing the serialized representation of this
     * object
     * @throws IOException
     */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(bytesOut);
        write(dataOut);

        return bytesOut.toByteArray();
    }

    /**
     * Creates a
     * <code>HMapSIW</code> object from a
     * <code>DataInput</code>.
     *
     * @param in source for reading the serialized representation
     * @return a newly-created
     * <code>OHMapSIW</code> object
     * @throws IOException
     */
    public static HMapSIW create(DataInputStream in) throws IOException {
        HMapSIW m = new HMapSIW();
        m.readFields(in);

        return m;
    }

    /**
     * Creates a
     * <code>HMapSIW</code> object from a byte array.
     *
     * @param bytes source for reading the serialized representation
     * @return a newly-created
     * <code>OHMapSIW</code> object
     * @throws IOException
     */
    public static HMapSIW create(byte[] bytes) throws IOException {
        return create(new DataInputStream(new ByteArrayInputStream(bytes)));
    }

    @Override
    public int compareTo(Object t) {
        throw new UnsupportedOperationException("Not supported yet.");
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

    @Override
    public void readFields(byte[] input, int offset) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public WritableComparable create(byte[] input, int offset) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
