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
package test.hone.pagerank;

import java.io.*;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.DynamicDirectByteBuffer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ArrayListOfIntsWritable;

/**
 * Representation of a graph node for PageRank.
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class PageRankNode implements WritableComparable {

    @Override
    public int compareTo(Object o) {
        PageRankNode other = (PageRankNode) o;
        if (this.pagerank > other.pagerank) {
            return 1;
        } else {
            return 0;
        }
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

    public static enum Type {

        Complete((byte) 0), // PageRank mass and adjacency list.
        Mass((byte) 1), // PageRank mass only.
        Structure((byte) 2); // Adjacency list only.
        public byte val;

        private Type(byte v) {
            this.val = v;
        }
    };
    private static final Type[] mapping = new Type[]{Type.Complete, Type.Mass, Type.Structure};
    private Type type;
    private int nodeid;
    private float pagerank;
    private ArrayListOfIntsWritable adjacenyList;

    public PageRankNode() {
    }

    public float getPageRank() {
        return pagerank;
    }

    public void setPageRank(float p) {
        this.pagerank = p;
    }

    public int getNodeId() {
        return nodeid;
    }

    public void setNodeId(int n) {
        this.nodeid = n;
    }

    public ArrayListOfIntsWritable getAdjacenyList() {
        return adjacenyList;
    }

    public void setAdjacencyList(ArrayListOfIntsWritable list) {
        this.adjacenyList = list;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    /**
     * Deserializes this object.
     *
     * @param in source for raw byte representation
     */
    @Override
    public void readFields(DataInputStream in) throws IOException {
        int b = in.readByte();
        type = mapping[b];
        nodeid = in.readInt();

        if (type.equals(Type.Mass)) {
            pagerank = in.readFloat();
            return;
        }

        if (type.equals(Type.Complete)) {
            pagerank = in.readFloat();
        }

        adjacenyList = new ArrayListOfIntsWritable();
        adjacenyList.readFields(in);
    }

    public static int readInt(final byte[] a, final int i) {
        //return ByteBuffer.wrap(Arrays.copyOfRange(a, i, j)).getInt();
        return a[i] << 24 | (a[i + 1] & 0x000000FF) << 16 | (a[i + 2] & 0x000000FF) << 8 | (a[i + 3] & 0x000000FF);
    }

    // Quick, low-overhead conversion taking advantage of knowledge of
// how Java effects these conversions
    public static float readFloat(byte[] b, int offset) {
        if (b.length != 4) {
            return 0.0F;
        }

// Same as DataInputStream's 'readInt' method
        int i = readInt(b, offset);

// This converts bits as follows:
/*
         * int s = ((i >> 31) == 0) ? 1 : -1; int e = ((i >> 23) & 0xff); int m
         * = (e == 0) ? (i & 0x7fffff) << 1 : (i & 0x7fffff) | 0x800000;
         */

        return Float.intBitsToFloat(i);
    }

    /**
     * Deserializes this object.
     *
     * @param in source for raw byte representation
     */
    public void readFields(byte[] a, int offset) throws IOException {
        int b = a[offset];
        type = mapping[b];
        offset++;
        nodeid = readInt(a, offset);
        offset += 4;

        if (type.equals(Type.Mass)) {
            pagerank = readFloat(a, offset);
            offset += 4;
            return;
        }

        if (type.equals(Type.Complete)) {
            pagerank = readFloat(a, offset);
            offset += 4;
        }

        adjacenyList = new ArrayListOfIntsWritable();
        adjacenyList.readFields(a, offset);
    }

    /**
     * Serializes this object.
     *
     * @param out where to write the raw byte representation
     */
    @Override
    public int write(DataOutputStream out) throws IOException {

        int noBytesWritten = 0;

        out.writeByte(type.val);
        noBytesWritten++;
        out.writeInt(nodeid);
        noBytesWritten += 4;

        if (type.equals(Type.Mass)) {
            out.writeFloat(pagerank);
            noBytesWritten += 4;
            return noBytesWritten;
        }

        if (type.equals(Type.Complete)) {
            out.writeFloat(pagerank);
            noBytesWritten += 4;
        }

        adjacenyList.write(out);
        noBytesWritten += 4 * (1 + adjacenyList.size());

        return noBytesWritten;
    }

    @Override
    public int write(ByteBuffer buf) {

        int noBytesWritten = 0;

        buf.put(type.val);
        noBytesWritten++;
        buf.putInt(nodeid);
        noBytesWritten += 4;

        if (type.equals(Type.Mass)) {
            buf.putFloat(pagerank);
            noBytesWritten += 4;
            return noBytesWritten;
        }

        if (type.equals(Type.Complete)) {
            buf.putFloat(pagerank);
            noBytesWritten += 4;
        }

        adjacenyList.write(buf);
        noBytesWritten += 4 * (1 + adjacenyList.size());

        return noBytesWritten;
    }

    @Override
    public int write(DynamicDirectByteBuffer buf) {

        int noBytesWritten = 0;

        buf.put(type.val);
        noBytesWritten++;
        buf.putInt(nodeid);
        noBytesWritten += 4;

        if (type.equals(Type.Mass)) {
            buf.putFloat(pagerank);
            noBytesWritten += 4;
            return noBytesWritten;
        }

        if (type.equals(Type.Complete)) {
            buf.putFloat(pagerank);
            noBytesWritten += 4;
        }

        adjacenyList.write(buf);
        noBytesWritten += 4 * (1 + adjacenyList.size());

        return noBytesWritten;
    }

    @Override
    public String toString() {
        return String.format("{%d %.4f %s}",
                nodeid, pagerank, (adjacenyList == null ? "[]" : adjacenyList.toString(10)));
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
     * Creates object from a
     * <code>DataInput</code>.
     *
     * @param in source for reading the serialized representation
     * @return newly-created object
     * @throws IOException
     */
    public static PageRankNode create(DataInputStream in) throws IOException {
        PageRankNode m = new PageRankNode();
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
    public static PageRankNode create(byte[] bytes) throws IOException {
        return create(new DataInputStream(new ByteArrayInputStream(bytes)));
    }

    public PageRankNode create(byte[] bytes, int offset) throws IOException {
        PageRankNode m = new PageRankNode();
        m.readFields(bytes, offset);
        return m;
    }
}
