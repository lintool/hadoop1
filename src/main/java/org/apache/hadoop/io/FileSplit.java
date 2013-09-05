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
import java.nio.charset.CharacterCodingException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ByteUtil;

/**
 * A section of an input file. Returned by {@link
 * InputFormat#getSplits(JobContext)} and passed to
 * {@link InputFormat#createRecordReader(InputSplit,TaskAttemptContext)}.
 */
public class FileSplit extends InputSplit implements Writable {

    private String file;
    private long start;
    private long length;
    private String[] hosts;

    FileSplit() {
    }

    /**
     * Constructs a split with host information
     *
     * @param file the file name
     * @param start the position of the first byte in the file to process
     * @param length the number of bytes in the file to process
     * @param hosts the list of hosts containing the block, possibly null
     */
    public FileSplit(String file, long start, long length, String[] hosts) {
        this.file = file;
        this.start = start;
        this.length = length;
        this.hosts = hosts;
    }

    /**
     * The file containing this split's data.
     */
    public String getPath() {
        return file;
    }

    /**
     * The position of the first byte in the file to process.
     */
    public long getStart() {
        return start;
    }

    /**
     * The number of bytes in the file to process.
     */
    @Override
    public long getLength() {
        return length;
    }

    @Override
    public String toString() {
        return file + ":" + start + "+" + length;
    }

    ////////////////////////////////////////////
    // Writable methods
    ////////////////////////////////////////////
    @Override
    public int write(DataOutputStream out) throws IOException {
        Text.writeString(out, file);
        out.writeLong(start);
        out.writeLong(length);
        return file.length() + 1 + 8;
    }

    @Override
    public void readFields(DataInputStream in) throws IOException {
        file = Text.readString(in);
        start = in.readLong();
        length = in.readLong();
        hosts = null;
    }

    @Override
    public String[] getLocations() throws IOException {
        if (this.hosts == null) {
            return new String[]{};
        } else {
            return this.hosts;
        }
    }

    @Override
    public int compare(byte[] a, int i1, int j1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int write(ByteBuffer buf) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void readFields(byte[] input, int offset) throws IOException {
        file = ByteUtil.readString(input, offset + Text.offset, input[offset + Text.offset - 1] & ByteUtil.MASK);
        start = ByteUtil.readLong(input, offset);
        length = ByteUtil.readLong(input, offset);
        hosts = null;
    }

    @Override
    public FileSplit create(byte[] input, int offset) throws IOException {
        FileSplit m = new FileSplit();
        m.readFields(input, offset);
        return m;
    }
}
