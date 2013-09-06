package test.hone.mrlda;

import edu.umd.cloud9.util.map.HMapII;
import edu.umd.cloud9.util.map.MapII;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.hadoop.io.DynamicDirectByteBuffer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ByteUtil;

public class Document implements WritableComparable {

    /**
     *
     */
    private static final long serialVersionUID = 752244298258266755L;
    /**
     *
     */
    private HMapII content = null;
    /**
     * @deprecated
     */
    private double[] gamma = null;
    /**
     * Define the total number of words in this document, not necessarily
     * distinct.
     */
    private int numberOfWords = 0;

    /**
     * Creates a
     * <code>LDADocument</code> object from a byte array.
     *
     * @param bytes raw serialized representation
     * @return a newly-created
     * <code>LDADocument</code> object
     * @throws IOException
     */
    public static Document create(byte[] bytes) throws IOException {
        return create(new DataInputStream(new ByteArrayInputStream(bytes)));
    }

    /**
     * Creates a
     * <code>LDADocument</code> object from a
     * <code>DataInput</code>.
     *
     * @param in source for reading the serialized representation
     * @return a newly-created
     * <code>LDADocument</code> object
     * @throws IOException
     */
    public static Document create(DataInputStream in) throws IOException {
        Document m = new Document();
        m.readFields(in);

        return m;
    }

    /**
     * Deserializes the LDADocument.
     *
     * @param in source for raw byte representation
     */
    public void readFields(byte[] input, int offset) throws IOException {
        numberOfWords = 0;

        int numEntries = ByteUtil.readInt(input, offset);
        offset += 4;
        if (numEntries <= 0) {
            content = null;
        } else {
            content = new HMapII();
            for (int i = 0; i < numEntries; i++) {
                int id = ByteUtil.readInt(input, offset);
                offset += 4;
                int count = ByteUtil.readInt(input, offset);
                offset += 4;
                content.put(id, count);
                numberOfWords += count;
            }
        }

        int numTopics = ByteUtil.readInt(input, offset);
        offset += 4;
        if (numTopics <= 0) {
            gamma = null;
        } else {
            gamma = new double[numTopics];
            for (int i = 0; i < numTopics; i++) {
                gamma[i] = ByteUtil.readDouble(input, offset);
                offset += 8;
            }
        }
    }

    public Document create(byte[] input, int offset) throws IOException {
        Document m = new Document();
        m.readFields(input, offset);

        return m;
    }

    public Document() {
    }

    public Document(HMapII document) {
        this.content = document;
        if (document != null) {
            Iterator<Integer> itr = this.content.values().iterator();
            while (itr.hasNext()) {
                numberOfWords += itr.next();
            }
        }
    }

    /**
     * @deprecated @param document
     * @param gamma
     */
    public Document(HMapII document, double[] gamma) {
        this(document);
        this.gamma = gamma;
    }

    /**
     * @deprecated @param document
     * @param numberOfTopics
     */
    public Document(HMapII document, int numberOfTopics) {
        this(document, new double[numberOfTopics]);
    }

    public HMapII getContent() {
        return this.content;
    }

    /**
     * @deprecated @return
     */
    public double[] getGamma() {
        return gamma;
    }

    /**
     * @deprecated @return
     */
    public int getNumberOfTopics() {
        if (gamma == null) {
            return 0;
        } else {
            return gamma.length;
        }
    }

    /**
     * Get the total number of distinct types in this document.
     *
     * @return the total number of unique types in this document.
     */
    public int getNumberOfTypes() {
        if (content == null) {
            return 0;
        } else {
            return content.size();
        }
    }

    /**
     * Get the total number of words in this document, not necessarily distinct.
     *
     * @return the total number of words in this document, not necessarily
     * distinct.
     */
    public int getNumberOfWords() {
        return numberOfWords;
    }

    /**
     * Deserializes the LDADocument.
     *
     * @param in source for raw byte representation
     */
    public void readFields(DataInputStream in) throws IOException {
        numberOfWords = 0;

        int numEntries = in.readInt();
        if (numEntries <= 0) {
            content = null;
        } else {
            content = new HMapII();
            for (int i = 0; i < numEntries; i++) {
                int id = in.readInt();
                int count = in.readInt();
                content.put(id, count);
                numberOfWords += count;
            }
        }

        int numTopics = in.readInt();
        if (numTopics <= 0) {
            gamma = null;
        } else {
            gamma = new double[numTopics];
            for (int i = 0; i < numTopics; i++) {
                gamma[i] = in.readDouble();
            }
        }
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

    public void setDocument(HMapII document) {
        this.content = document;
        numberOfWords = 0;

        if (document != null) {
            Iterator<Integer> itr = this.content.values().iterator();
            while (itr.hasNext()) {
                numberOfWords += itr.next();
            }
        }
    }

    /**
     * @deprecated @param gamma
     */
    public void setGamma(double[] gamma) {
        this.gamma = gamma;
    }

    @Override
    public String toString() {
        StringBuilder document = new StringBuilder("content:\t");
        if (content == null) {
            document.append("null");
        } else {
            Iterator<Integer> itr = this.content.keySet().iterator();
            while (itr.hasNext()) {
                int id = itr.next();
                document.append(id);
                document.append(":");
                document.append(content.get(id));
                document.append(" ");
            }
        }
        document.append("\ngamma:\t");
        if (gamma == null) {
            document.append("null");
        } else {
            for (double value : gamma) {
                document.append(value);
                document.append(" ");
            }
        }

        return document.toString();
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
        if (content == null) {
            out.writeInt(0);
            bytesWritten += 4;
        } else {
            out.writeInt(content.size());
            bytesWritten += 4;
            for (MapII.Entry e : content.entrySet()) {
                out.writeInt(e.getKey());
                bytesWritten += 4;
                out.writeInt(e.getValue());
                bytesWritten += 4;
            }
        }

        // Write out the gamma values for this document.
        if (gamma == null) {
            out.writeInt(0);
            bytesWritten += 4;
        } else {
            out.writeInt(gamma.length);
            bytesWritten += 4;
            for (double value : gamma) {
                // TODO: change it to double and also in read method
                out.writeDouble(value);
                bytesWritten += 8;
            }
        }

        return bytesWritten;
    }

    public int write(ByteBuffer buf) {
        // Write out the number of entries in the map.
        int bytesWritten = 0;
        if (content == null) {
            buf.putInt(0);
            bytesWritten += 4;
        } else {
            buf.putInt(content.size());
            bytesWritten += 4;
            for (MapII.Entry e : content.entrySet()) {
                buf.putInt(e.getKey());
                bytesWritten += 4;
                buf.putInt(e.getValue());
                bytesWritten += 4;
            }
        }

        // Write out the gamma values for this document.
        if (gamma == null) {
            buf.putInt(0);
            bytesWritten += 4;
        } else {
            buf.putInt(gamma.length);
            bytesWritten += 4;
            for (double value : gamma) {
                // TODO: change it to double and also in read method
                buf.putDouble(value);
                bytesWritten += 8;
            }
        }

        return bytesWritten;
    }

    public int write(DynamicDirectByteBuffer buf) {
        // Write out the number of entries in the map.
        int bytesWritten = 0;
        if (content == null) {
            buf.putInt(0);
            bytesWritten += 4;
        } else {
            buf.putInt(content.size());
            bytesWritten += 4;
            for (MapII.Entry e : content.entrySet()) {
                buf.putInt(e.getKey());
                bytesWritten += 4;
                buf.putInt(e.getValue());
                bytesWritten += 4;
            }
        }

        // Write out the gamma values for this document.
        if (gamma == null) {
            buf.putInt(0);
            bytesWritten += 4;
        } else {
            buf.putInt(gamma.length);
            bytesWritten += 4;
            for (double value : gamma) {
                // TODO: change it to double and also in read method
                buf.putDouble(value);
                bytesWritten += 8;
            }
        }

        return bytesWritten;
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
}
