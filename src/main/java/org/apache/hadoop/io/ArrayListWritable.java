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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.util.ByteUtil;

/**
 * <p> Writable extension of a Java ArrayList. Elements in the list must be
 * homogeneous and must implement Hadoop's Writable interface. </p>
 *
 * @param <E> type of list element
 *
 * @author Jimmy Lin
 * @author Tamer Elsayed
 */
public class ArrayListWritable<E extends WritableComparable> extends ArrayList<E> implements WritableComparable {

    private static final long serialVersionUID = 4911321393319821791L;

    /**
     * Creates an ArrayListWritable object.
     */
    public ArrayListWritable() {
        super();
    }

    /**
     * Creates an ArrayListWritable object from an ArrayList.
     */
    public ArrayListWritable(List<E> array) {
        super(array);
    }

    /**
     * Deserializes the array.
     *
     * @param in source for raw byte representation
     */
    @SuppressWarnings("unchecked")
    public void readFields(DataInputStream in) throws IOException {
        this.clear();

        int numFields = in.readInt();
        if (numFields == 0) {
            return;
        }
        String className = in.readUTF();
        E obj;
        try {
            Class<E> c = (Class<E>) Class.forName(className);
            for (int i = 0; i < numFields; i++) {
                obj = (E) c.newInstance();
                obj.readFields(in);
                this.add(obj);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Serializes this array.
     *
     * @param out where to write the raw byte representation
     */
    public int write(DataOutputStream out) throws IOException {
        out.writeInt(this.size());
        if (size() == 0) {
            return 0;
        }
        E obj = get(0);

        out.writeUTF(obj.getClass().getCanonicalName());

        for (int i = 0; i < size(); i++) {
            obj = get(i);
            if (obj == null) {
                throw new IOException("Cannot serialize null fields!");
            }
            obj.write(out);
        }

        return 0;
    }

    public int write(ByteBuffer buf) {
        buf.putInt(this.size());
        if (size() == 0) {
            return 0;
        }
        E obj = get(0);

        byte b = 0;
        buf.put(b);
        b = (byte) obj.getClass().getCanonicalName().length();
        buf.put(b);
        CharBuffer cbuf = buf.asCharBuffer();
        cbuf.put(obj.getClass().getCanonicalName());
        Charset charset = Charset.forName("UTF-8");
        CharsetEncoder encoder = charset.newEncoder();
        cbuf.flip();
        ByteBuffer buf1 = null;
        try {
            buf1 = encoder.encode(cbuf);
        } catch (CharacterCodingException ex) {
            Logger.getLogger(ArrayListWritable.class.getName()).log(Level.SEVERE, null, ex);
        }
        buf.put(buf1);

        for (int i = 0; i < size(); i++) {
            obj = get(i);
            if (obj == null) {
                try {
                    throw new IOException("Cannot serialize null fields!");
                } catch (IOException ex) {
                    Logger.getLogger(ArrayListWritable.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            obj.write(buf);
        }

        return 0;
    }

    public int write(DynamicDirectByteBuffer buf) {
        buf.putInt(this.size());
        if (size() == 0) {
            return 0;
        }
        E obj = get(0);
        try {
            buf.putString(obj.getClass().getCanonicalName());
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(ArrayListWritable.class.getName()).log(Level.SEVERE, null, ex);
        }

        for (int i = 0; i < size(); i++) {
            obj = get(i);
            if (obj == null) {
                try {
                    throw new IOException("Cannot serialize null fields!");
                } catch (IOException ex) {
                    Logger.getLogger(ArrayListWritable.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            obj.write(buf);
        }

        return 0;
    }

    /**
     * Generates human-readable String representation of this ArrayList.
     *
     * @return human-readable String representation of this ArrayList
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append('[');
        for (int i = 0; i < this.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(this.get(i));
        }
        sb.append(']');

        return sb.toString();
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
    public int compareTo(Object o) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void set(WritableComparable obj) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void readFields(byte[] input, int offset) throws IOException {
        this.clear();

        int numFields = ByteUtil.readInt(input, offset);
        offset = offset + 4;
        if (numFields == 0) {
            return;
        }
        String className = ByteUtil.readString(input, offset + Text.offset, input[offset + Text.offset - 1] & ByteUtil.MASK);
        E obj;
        try {
            Class<E> c = (Class<E>) Class.forName(className);
            for (int i = 0; i < numFields; i++) {
                obj = (E) (c.newInstance()).create(input, offset);
                this.add(obj);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public ArrayListWritable create(byte[] input, int offset) throws IOException {
        ArrayListWritable m = new ArrayListWritable();
        m.readFields(input, offset);
        return m;
    }
}
