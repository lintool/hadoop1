/**
 *
 */
package org.apache.hadoop.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

/**
 * The basic functionality to indicate serialization This differs from the
 * Hadoop signature because it takes simple Java streams, but the purpose of
 * mapreduce4j is
 *
 * @author tim
 * @param <T>
 */
public interface WritableComparable<T> extends Writable<T>, Comparable<T> {

    @Override
    int write(DataOutputStream out) throws IOException;
    
    int write(ByteBuffer buf);

    int write(DynamicDirectByteBuffer buf);

    @Override
    void readFields(DataInputStream in) throws IOException;
    
    @Override
    void readFields(byte[] input, int offset) throws IOException;
    
    @Override
    WritableComparable create(byte[] input, int offset) throws IOException;

    @Override
    int hashCode();
    
    @Override
    int compare(byte[] a, int i1, int j1);
    
    int getOffset();
    
    void set(WritableComparable obj);
}
