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
 */
public interface Writable<T> {

    int write(DataOutputStream out) throws IOException;
    
    int write(ByteBuffer buf);
    
    void readFields(DataInputStream in) throws IOException;
    
    void readFields(byte[] input, int offset) throws IOException;

    int hashCode();

    int compare(byte[] a, int i1, int j1);
    
    Writable create(byte[] input, int offset) throws IOException;

}
