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

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * <code>SequenceFile</code>s are flat files consisting of binary key/value
 * pairs.
 *
 * <p><code>SequenceFile</code> provides {@link Writer}, {@link Reader} and
 * {@link Sorter} classes for writing, reading and sorting respectively.</p>
 *
 * There are three
 * <code>SequenceFile</code>
 * <code>Writer</code>s based on the
 * {@link CompressionType} used to compress key/value pairs: <ol> <li>
 * <code>Writer</code> : Uncompressed records. </li> <li>
 * <code>RecordCompressWriter</code> : Record-compressed files, only compress
 * values. </li> <li>
 * <code>BlockCompressWriter</code> : Block-compressed files, both keys & values
 * are collected in 'blocks' separately and compressed. The size of the 'block'
 * is configurable. </ol>
 *
 * <p>The actual compression algorithm used to compress key and/or values can be
 * specified by using the appropriate {@link CompressionCodec}.</p>
 *
 * <p>The recommended way is to use the static <tt>createWriter</tt> methods
 * provided by the
 * <code>SequenceFile</code> to chose the preferred format.</p>
 *
 * <p>The {@link Reader} acts as the bridge and can read any of the above
 * <code>SequenceFile</code> formats.</p>
 *
 * <h4 id="Formats">SequenceFile Formats</h4>
 *
 * <p>Essentially there are 3 different formats for
 * <code>SequenceFile</code>s depending on the
 * <code>CompressionType</code> specified. All of them share a <a
 * href="#Header">common header</a> described below.
 *
 * <h5 id="Header">SequenceFile Header</h5> <ul> <li> version - 3 bytes of magic
 * header <b>SEQ</b>, followed by 1 byte of actual version number (e.g. SEQ4 or
 * SEQ6) </li> <li> keyClassName -key class </li> <li> valueClassName - value
 * class </li> <li> compression - A boolean which specifies if compression is
 * turned on for keys/values in this file. </li> <li> blockCompression - A
 * boolean which specifies if block-compression is turned on for keys/values in
 * this file. </li> <li> compression codec -
 * <code>CompressionCodec</code> class which is used for compression of keys
 * and/or values (if compression is enabled). </li> <li> metadata - {@link Metadata}
 * for this file. </li> <li> sync - A sync marker to denote end of the header.
 * </li> </ul>
 *
 * <h5 id="#UncompressedFormat">Uncompressed SequenceFile Format</h5> <ul> <li>
 * <a href="#Header">Header</a> </li> <li> Record <ul> <li>Record length</li>
 * <li>Key length</li> <li>Key</li> <li>Value</li> </ul> </li> <li> A
 * sync-marker every few
 * <code>100</code> bytes or so. </li> </ul>
 *
 * <h5 id="#RecordCompressedFormat">Record-Compressed SequenceFile Format</h5>
 * <ul> <li> <a href="#Header">Header</a> </li> <li> Record <ul> <li>Record
 * length</li> <li>Key length</li> <li>Key</li> <li><i>Compressed</i> Value</li>
 * </ul> </li> <li> A sync-marker every few
 * <code>100</code> bytes or so. </li> </ul>
 *
 * <h5 id="#BlockCompressedFormat">Block-Compressed SequenceFile Format</h5>
 * <ul> <li> <a href="#Header">Header</a> </li> <li> Record <i>Block</i> <ul>
 * <li>Compressed key-lengths block-size</li> <li>Compressed key-lengths
 * block</li> <li>Compressed keys block-size</li> <li>Compressed keys block</li>
 * <li>Compressed value-lengths block-size</li> <li>Compressed value-lengths
 * block</li> <li>Compressed values block-size</li> <li>Compressed values
 * block</li> </ul> </li> <li> A sync-marker every few
 * <code>100</code> bytes or so. </li> </ul>
 *
 * <p>The compressed blocks of key lengths and value lengths consist of the
 * actual lengths of individual keys/values encoded in ZeroCompressedInteger
 * format.</p>
 *
 * @see CompressionCodec
 */
public class SequenceFile {

    private SequenceFile() {
    }                         // no public ctor
    private static final byte BLOCK_COMPRESS_VERSION = (byte) 4;
    private static final byte CUSTOM_COMPRESS_VERSION = (byte) 5;
    private static final byte VERSION_WITH_METADATA = (byte) 6;
    private static byte[] VERSION = new byte[]{
        (byte) 'S', (byte) 'E', (byte) 'Q', VERSION_WITH_METADATA
    };
    private static final int SYNC_ESCAPE = -1;      // "length" of sync entries
    private static final int SYNC_HASH_SIZE = 16;   // number of bytes in hash 
    private static final int SYNC_SIZE = 4 + SYNC_HASH_SIZE; // escape + hash
    /**
     * The number of bytes between sync points.
     */
    public static final int SYNC_INTERVAL = 100 * SYNC_SIZE;

    /**
     * The compression type used to compress key/value pairs in the
     * {@link SequenceFile}.
     *
     * @see SequenceFile.Writer
     */
    public static enum CompressionType {

        /**
         * Do not compress records.
         */
        NONE,
        /**
         * Compress values only, each separately.
         */
        RECORD,
        /**
         * Compress sequences of records together in blocks.
         */
        BLOCK
    }

    /**
     * Get the compression type for the reduce outputs
     *
     * @param job the job config to look in
     * @return the kind of compression to use
     * @deprecated Use
     *             {@link org.apache.hadoop.mapred.SequenceFileOutputFormat#getOutputCompressionType(org.apache.hadoop.mapred.JobConf)}
     * to get {@link CompressionType} for job-outputs.
     */
    @Deprecated
    static public CompressionType getCompressionType(Configuration job) {
        String name = job.get("io.seqfile.compression.type");
        return name == null ? CompressionType.RECORD
                : CompressionType.valueOf(name);
    }

    /**
     * Set the compression type for sequence files.
     *
     * @param job the configuration to modify
     * @param val the new compression type (none, block, record)
     * @deprecated Use the one of the many SequenceFile.createWriter methods to
     * specify the {@link CompressionType} while creating the {@link SequenceFile}
     * or
     *             {@link org.apache.hadoop.mapred.SequenceFileOutputFormat#setOutputCompressionType(org.apache.hadoop.mapred.JobConf, org.apache.hadoop.io.SequenceFile.CompressionType)}
     * to specify the {@link CompressionType} for job-outputs. or
     */
    @Deprecated
    static public void setCompressionType(Configuration job,
            CompressionType val) {
        job.set("io.seqfile.compression.type", val.toString());
    }

    /**
     * Construct the preferred type of SequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @return Returns the handle to the constructed SequenceFile Writer.
     * @throws IOException
     */
    public static Writer createWriter(Configuration conf, Path name,
            Class keyClass, Class valClass)
            throws IOException {
        return createWriter(conf, name, keyClass, valClass,
                getCompressionType(conf));
    }

    /**
     * Construct the preferred type of SequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compressionType The compression type.
     * @return Returns the handle to the constructed SequenceFile Writer.
     * @throws IOException
     */
    public static Writer createWriter(Configuration conf, Path name,
            Class keyClass, Class valClass, CompressionType compressionType)
            throws IOException {
        //return createWriter(fs, conf, name, keyClass, valClass,            fs.getConf().getInt("io.file.buffer.size", 4096),        fs.getDefaultReplication(), fs.getDefaultBlockSize(),          compressionType, new DefaultCodec(), null, new Metadata());
        return null;
    }

    /**
     * Construct the preferred type of SequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compressionType The compression type.
     * @param progress The Progressable object to track progress.
     * @return Returns the handle to the constructed SequenceFile Writer.
     * @throws IOException
     */
//  public static Writer
//    createWriter(FileSystem fs, Configuration conf, Path name, 
//                 Class keyClass, Class valClass, CompressionType compressionType,
//                 Progressable progress) throws IOException {
//    return createWriter(fs, conf, name, keyClass, valClass,
//            fs.getConf().getInt("io.file.buffer.size", 4096),
//            fs.getDefaultReplication(), fs.getDefaultBlockSize(),
//            compressionType, new DefaultCodec(), progress, new Metadata());
//  }
    /**
     * Construct the preferred type of SequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @return Returns the handle to the constructed SequenceFile Writer.
     * @throws IOException
     */
//  public static Writer 
//    createWriter(FileSystem fs, Configuration conf, Path name, 
//                 Class keyClass, Class valClass, 
//                 CompressionType compressionType, CompressionCodec codec) 
//    throws IOException {
//    return createWriter(fs, conf, name, keyClass, valClass,
//            fs.getConf().getInt("io.file.buffer.size", 4096),
//            fs.getDefaultReplication(), fs.getDefaultBlockSize(),
//            compressionType, codec, null, new Metadata());
//  }
    /**
     * Construct the preferred type of SequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @param progress The Progressable object to track progress.
     * @param metadata The metadata of the file.
     * @return Returns the handle to the constructed SequenceFile Writer.
     * @throws IOException
     */
//  public static Writer
//    createWriter(FileSystem fs, Configuration conf, Path name, 
//                 Class keyClass, Class valClass, 
//                 CompressionType compressionType, CompressionCodec codec,
//                 Progressable progress, Metadata metadata) throws IOException {
//    return createWriter(fs, conf, name, keyClass, valClass,
//            fs.getConf().getInt("io.file.buffer.size", 4096),
//            fs.getDefaultReplication(), fs.getDefaultBlockSize(),
//            compressionType, codec, progress, metadata);
//  }
    /**
     * Construct the preferred type of SequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param bufferSize buffer size for the underlaying outputstream.
     * @param replication replication factor for the file.
     * @param blockSize block size for the file.
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @param progress The Progressable object to track progress.
     * @param metadata The metadata of the file.
     * @return Returns the handle to the constructed SequenceFile Writer.
     * @throws IOException
     */
//  public static Writer
//    createWriter(FileSystem fs, Configuration conf, Path name,
//                 Class keyClass, Class valClass, int bufferSize,
//                 short replication, long blockSize,
//                 CompressionType compressionType, CompressionCodec codec,
//                 Progressable progress, Metadata metadata) throws IOException {
//    if ((codec instanceof GzipCodec) &&
//        !NativeCodeLoader.isNativeCodeLoaded() &&
//        !ZlibFactory.isNativeZlibLoaded(conf)) {
//      throw new IllegalArgumentException("SequenceFile doesn't work with " +
//                                         "GzipCodec without native-hadoop code!");
//    }
//
//    Writer writer = null;
//
//    if (compressionType == CompressionType.NONE) {
//      writer = new Writer(fs, conf, name, keyClass, valClass,
//                          bufferSize, replication, blockSize,
//                          progress, metadata);
//    } else if (compressionType == CompressionType.RECORD) {
//      writer = new RecordCompressWriter(fs, conf, name, keyClass, valClass,
//                                        bufferSize, replication, blockSize,
//                                        codec, progress, metadata);
//    } else if (compressionType == CompressionType.BLOCK){
//      writer = new BlockCompressWriter(fs, conf, name, keyClass, valClass,
//                                       bufferSize, replication, blockSize,
//                                       codec, progress, metadata);
//    }
//
//    return writer;
//  }
    /**
     * Construct the preferred type of SequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param name The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @param progress The Progressable object to track progress.
     * @return Returns the handle to the constructed SequenceFile Writer.
     * @throws IOException
     */
//  public static Writer
//    createWriter(FileSystem fs, Configuration conf, Path name, 
//                 Class keyClass, Class valClass, 
//                 CompressionType compressionType, CompressionCodec codec,
//                 Progressable progress) throws IOException {
//    Writer writer = createWriter(fs, conf, name, keyClass, valClass, 
//                                 compressionType, codec, progress, new Metadata());
//    return writer;
//  }
    /**
     * Construct the preferred type of 'raw' SequenceFile Writer.
     *
     * @param out The stream on top which the writer is to be constructed.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compress Compress data?
     * @param blockCompress Compress blocks?
     * @param metadata The metadata of the file.
     * @return Returns the handle to the constructed SequenceFile Writer.
     * @throws IOException
     */
//  private static Writer
//    createWriter(Configuration conf, FSDataOutputStream out, 
//                 Class keyClass, Class valClass, boolean compress, boolean blockCompress,
//                 CompressionCodec codec, Metadata metadata)
//    throws IOException {
//    if (codec != null && (codec instanceof GzipCodec) && 
//        !NativeCodeLoader.isNativeCodeLoaded() && 
//        !ZlibFactory.isNativeZlibLoaded(conf)) {
//      throw new IllegalArgumentException("SequenceFile doesn't work with " +
//                                         "GzipCodec without native-hadoop code!");
//    }
//
//    Writer writer = null;
//
//    if (!compress) {
//      writer = new Writer(conf, out, keyClass, valClass, metadata);
//    } else if (compress && !blockCompress) {
//      writer = new RecordCompressWriter(conf, out, keyClass, valClass, codec, metadata);
//    } else {
//      writer = new BlockCompressWriter(conf, out, keyClass, valClass, codec, metadata);
//    }
//    
//    return writer;
//  }
    /**
     * Construct the preferred type of 'raw' SequenceFile Writer.
     *
     * @param fs The configured filesystem.
     * @param conf The configuration.
     * @param file The name of the file.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compress Compress data?
     * @param blockCompress Compress blocks?
     * @param codec The compression codec.
     * @param progress
     * @param metadata The metadata of the file.
     * @return Returns the handle to the constructed SequenceFile Writer.
     * @throws IOException
     */
//  private static Writer
//  createWriter(FileSystem fs, Configuration conf, Path file, 
//               Class keyClass, Class valClass, 
//               boolean compress, boolean blockCompress,
//               CompressionCodec codec, Progressable progress, Metadata metadata)
//  throws IOException {
//  if (codec != null && (codec instanceof GzipCodec) && 
//      !NativeCodeLoader.isNativeCodeLoaded() && 
//      !ZlibFactory.isNativeZlibLoaded(conf)) {
//    throw new IllegalArgumentException("SequenceFile doesn't work with " +
//                                       "GzipCodec without native-hadoop code!");
//  }
//
//  Writer writer = null;
//
//  if (!compress) {
//    writer = new Writer(fs, conf, file, keyClass, valClass, progress, metadata);
//  } else if (compress && !blockCompress) {
//    writer = new RecordCompressWriter(fs, conf, file, keyClass, valClass, 
//                                      codec, progress, metadata);
//  } else {
//    writer = new BlockCompressWriter(fs, conf, file, keyClass, valClass, 
//                                     codec, progress, metadata);
//  }
//  
//  return writer;
//}
    /**
     * Construct the preferred type of 'raw' SequenceFile Writer.
     *
     * @param conf The configuration.
     * @param out The stream on top which the writer is to be constructed.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @param metadata The metadata of the file.
     * @return Returns the handle to the constructed SequenceFile Writer.
     * @throws IOException
     */
//  public static Writer
//    createWriter(Configuration conf, FSDataOutputStream out, 
//                 Class keyClass, Class valClass, CompressionType compressionType,
//                 CompressionCodec codec, Metadata metadata)
//    throws IOException {
//    if ((codec instanceof GzipCodec) && 
//        !NativeCodeLoader.isNativeCodeLoaded() && 
//        !ZlibFactory.isNativeZlibLoaded(conf)) {
//      throw new IllegalArgumentException("SequenceFile doesn't work with " +
//                                         "GzipCodec without native-hadoop code!");
//    }
//
//    Writer writer = null;
//
//    if (compressionType == CompressionType.NONE) {
//      writer = new Writer(conf, out, keyClass, valClass, metadata);
//    } else if (compressionType == CompressionType.RECORD) {
//      writer = new RecordCompressWriter(conf, out, keyClass, valClass, codec, metadata);
//    } else if (compressionType == CompressionType.BLOCK){
//      writer = new BlockCompressWriter(conf, out, keyClass, valClass, codec, metadata);
//    }
//    
//    return writer;
//  }
    /**
     * Construct the preferred type of 'raw' SequenceFile Writer.
     *
     * @param conf The configuration.
     * @param out The stream on top which the writer is to be constructed.
     * @param keyClass The 'key' type.
     * @param valClass The 'value' type.
     * @param compressionType The compression type.
     * @param codec The compression codec.
     * @return Returns the handle to the constructed SequenceFile Writer.
     * @throws IOException
     */
//  public static Writer
//    createWriter(Configuration conf, FSDataOutputStream out, 
//                 Class keyClass, Class valClass, CompressionType compressionType,
//                 CompressionCodec codec)
//    throws IOException {
//    Writer writer = createWriter(conf, out, keyClass, valClass, compressionType,
//                                 codec, new Metadata());
//    return writer;
//  }
//  
    /**
     * The interface to 'raw' values of SequenceFiles.
     */
    public static interface ValueBytes {

        /**
         * Writes the uncompressed bytes to the outStream.
         *
         * @param outStream : Stream to write uncompressed bytes into.
         * @throws IOException
         */
        public void writeUncompressedBytes(DataOutputStream outStream)
                throws IOException;

        /**
         * Write compressed bytes to outStream. Note: that it will NOT compress
         * the bytes if they are not compressed.
         *
         * @param outStream : Stream to write compressed bytes into.
         */
        public void writeCompressedBytes(DataOutputStream outStream)
                throws IllegalArgumentException, IOException;

        /**
         * Size of stored data.
         */
        public int getSize();
    }

    private static class UncompressedBytes implements ValueBytes {

        private int dataSize;
        private byte[] data;

        private UncompressedBytes() {
            data = null;
            dataSize = 0;
        }

        private void reset(DataInputStream in, int length) throws IOException {
            data = new byte[length];
            dataSize = -1;

            in.readFully(data);
            dataSize = data.length;
        }

        public int getSize() {
            return dataSize;
        }

        public void writeUncompressedBytes(DataOutputStream outStream)
                throws IOException {
            outStream.write(data, 0, dataSize);
        }

        public void writeCompressedBytes(DataOutputStream outStream)
                throws IllegalArgumentException, IOException {
            throw new IllegalArgumentException("UncompressedBytes cannot be compressed!");
        }
    } // UncompressedBytes

    public static class Writer implements java.io.Closeable {

        private DataOutputStream out;
        private ByteArrayOutputStream_test baos;
        private FileOutputStream fos;

        public Writer(Configuration conf, Path path, final Class<? extends WritableComparable> keyClass, final Class<? extends WritableComparable> valueClass) throws FileNotFoundException {
            baos = new ByteArrayOutputStream_test();
            out = new DataOutputStream(new BufferedOutputStream(baos));
            fos = new FileOutputStream(new File(path.toString()));
        }

        public synchronized void close() throws IOException {
            // Return the decompressors to the pool         

            // Close the input-stream
            out.flush();
            out.close();
            baos.flush();
            fos.write(baos.toByteArray());
            fos.flush();
            fos.close();
            baos.close();
        }

        public synchronized boolean next(Writable key) throws IOException {

            return true;
        }

        public synchronized void append(Writable key, Writable value) throws IOException {
            key.write(out);
            value.write(out);
        }
    }

    /**
     * Reads key/value pairs from a sequence-format file.
     */
    public static class Reader implements java.io.Closeable {

        private Path file;
        private DataOutputBuffer outBuf = new DataOutputBuffer();
        private byte version;
        private String keyClassName;
        private String valClassName;
        private Class keyClass;
        private Class valClass;
//    private CompressionCodec codec = null;
//    private Metadata metadata = null;
//    
        private byte[] sync = new byte[SYNC_HASH_SIZE];
        private byte[] syncCheck = new byte[SYNC_HASH_SIZE];
        private boolean syncSeen;
        private long end;
        private int keyLength;
        private int recordLength;
        private boolean decompress;
        private boolean blockCompressed;
        private Configuration conf;
        private int noBufferedRecords = 0;
        private boolean lazyDecompress = true;
        private boolean valuesDecompressed = true;
        private int noBufferedKeys = 0;
        private int noBufferedValues = 0;
        private DataInputStream in;
        private ByteArrayInputStream bais;
        private FileInputStream fis;
        private byte[] fileContent;

        /**
         * Open the named file.
         */
        public Reader(Path file, Configuration conf)
                throws IOException {
            File fileDesc = new File(file.toString());
            fis = new FileInputStream(fileDesc);
            fileContent = new byte[(int) fileDesc.length()];
            fis.read(fileContent);
            bais = new ByteArrayInputStream(fileContent);
            in = new DataInputStream(new BufferedInputStream(bais));
        }

        private Reader(Path file, int bufferSize,
                Configuration conf, boolean tempReader) throws IOException {
            this(file, bufferSize, 0, 0, conf, tempReader);
        }

        private Reader(Path file, int bufferSize, long start,
                long length, Configuration conf, boolean tempReader)
                throws IOException {
        }

        /**
         * Override this method to specialize the type of
         * {@link FSDataInputStream} returned.
         */
        protected DataInputStream openFile(Path file,
                int bufferSize, long length) throws IOException {
            return null;
        }

        /**
         * Initialize the {@link Reader}
         *
         * @param tmpReader
         * <code>true</code> if we are constructing a temporary reader {@link SequenceFile.Sorter.cloneFileAttributes},
         * and hence do not initialize every component;
         * <code>false</code> otherwise.
         * @throws IOException
         */
        private void init(boolean tempReader) throws IOException {
        }

        @SuppressWarnings("unchecked")
//        private Deserializer getDeserializer(SerializationFactory sf, Class c) {
//            return sf.getDeserializer(c);
//        }
        /**
         * Close the file.
         */
        public synchronized void close() throws IOException {
            // Return the decompressors to the pool


            // Close the input-stream
            in.close();
        }

        /**
         * Returns the name of the key class.
         */
        public String getKeyClassName() {
            return keyClassName;
        }

        /**
         * Returns the class of keys in this file.
         */
        public synchronized Class<?> getKeyClass() {
            return keyClass;
        }

        /**
         * Returns the name of the value class.
         */
        public String getValueClassName() {
            return valClassName;
        }

        /**
         * Returns the class of values in this file.
         */
        public synchronized Class<?> getValueClass() {

            return valClass;
        }

        /**
         * Returns true if values are compressed.
         */
        public boolean isCompressed() {
            return decompress;
        }

        /**
         * Returns true if records are block-compressed.
         */
        public boolean isBlockCompressed() {
            return blockCompressed;
        }

        /**
         * Returns the compression codec of data in this file.
         */
//        public CompressionCodec getCompressionCodec() {
//            return codec;
//        }
//
//        /**
//         * Returns the metadata object of the file
//         */
//        public Metadata getMetadata() {
//            return this.metadata;
//        }
        /**
         * Returns the configuration used for this file.
         */
        Configuration getConf() {
            return conf;
        }

        /**
         * Read a compressed buffer
         */
        private synchronized void readBuffer() throws IOException {
            // Read data into a temporary buffer
        }

        /**
         * Read the next 'compressed' block
         */
        private synchronized void readBlock() throws IOException {
            // Check if we need to throw away a whole block of 
            // 'values' due to 'lazy decompression' 
        }

        /**
         * Position valLenIn/valIn to the 'value' corresponding to the 'current'
         * key
         */
        private synchronized void seekToCurrentValue() throws IOException {
        }

        /**
         * Get the 'value' corresponding to the last read 'key'.
         *
         * @param val : The 'value' to be read.
         * @throws IOException
         */
        public synchronized void getCurrentValue(Writable val)
                throws IOException {
            val.readFields(in);
        }

        /**
         * Get the 'value' corresponding to the last read 'key'.
         *
         * @param val : The 'value' to be read.
         * @throws IOException
         */
        public synchronized Object getCurrentValue(Object val)
                throws IOException {
            return null;

        }

        @SuppressWarnings("unchecked")
        private Object deserializeValue(Object val) throws IOException {
            return null;
        }

        /**
         * Read the next key in the file into
         * <code>key</code>, skipping its value. True if another entry exists,
         * and false at end of file.
         */
        public synchronized boolean next(Writable key) throws IOException {
            if (in.available() > 0) {
                key.readFields(in);
                return true;
            } else {
                return false;
            }
        }

        /**
         * Read the next key/value pair in the file into
         * <code>key</code> and
         * <code>val</code>. Returns true if such a pair exists and false when
         * at end of file
         */
        public synchronized boolean next(Writable key, Writable val)
                throws IOException {
//            if (val.getClass() != getValueClass()) {
//                throw new IOException("wrong value class: " + val + " is not " + valClass);
//            }

            boolean more = next(key);

            if (more) {
                getCurrentValue(val);
            }

            return more;
        }

        /**
         * Read and return the next record length, potentially skipping over a
         * sync block.
         *
         * @return the length of the next record or -1 if there is no next
         * record
         * @throws IOException
         */
        private synchronized int readRecordLength() throws IOException {

            return 0;
        }

        /**
         * Read the next key/value pair in the file into
         * <code>buffer</code>. Returns the length of the key read, or -1 if at
         * end of file. The length of the value may be computed by calling
         * buffer.getLength() before and after calls to this method.
         */
        /**
         * @deprecated Call {@link #nextRaw(DataOutputBuffer,SequenceFile.ValueBytes)}.
         */
        public synchronized int next(DataOutputBuffer buffer) throws IOException {
            // Unsupported for block-compressed sequence files

            return 0;
        }
        /**
         * Read 'raw' records.
         *
         * @param key - The buffer into which the key is read
         * @param val - The 'raw' value
         * @return Returns the total record length or -1 for end of file
         * @throws IOException
         */
        // SequenceFile.Sorter
    } // SequenceFile
}
