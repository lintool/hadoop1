/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Singleton Writable with no data.
 */
public class NullWritable implements WritableComparable {

    private static final NullWritable THIS = new NullWritable();

    private NullWritable() {
    }                       // no public ctor

    /**
     * Returns the single instance of this class.
     */
    public static NullWritable get() {
        return THIS;
    }

    public String toString() {
        return "(null)";
    }

    public int hashCode() {
        return 0;
    }

    public int compareTo(Object other) {
        if (!(other instanceof NullWritable)) {
            throw new ClassCastException("can't compare " + other.getClass().getName()
                    + " to NullWritable");
        }
        return 0;
    }

    public boolean equals(Object other) {
        return other instanceof NullWritable;
    }

    @Override
    public void readFields(DataInputStream in) throws IOException {
    }

    @Override
    public int write(DataOutputStream out) throws IOException {
        return -1;
    }

    @Override
    public int write(ByteBuffer buf) {
        return -1;
    }

    @Override
    public int write(DynamicDirectByteBuffer buf) {
        return -1;
    }

    /**
     * Compare the buffers in serialized form.
     */
    public int compare(byte[] b1, int s1, int l1,
            byte[] b2, int s2, int l2) {
        assert 0 == l1;
        assert 0 == l2;
        return 0;
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
