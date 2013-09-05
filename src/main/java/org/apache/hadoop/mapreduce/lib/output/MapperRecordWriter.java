/**
 *
 */
package org.apache.hadoop.mapreduce.lib.output;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.ByteArrayOutputStream_test;
import org.apache.hadoop.io.DynamicDirectByteBuffer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.ByteQuickSort;
import org.apache.hadoop.util.DirectByteBufferCleaner;
import org.apache.hadoop.util.MCSLock;
import org.apache.hadoop.util.SpinLock;
import org.apache.hadoop.util.UnsafeDirectByteBuffer;
import sun.misc.Cleaner;

/**
 * Collects in memory during writing, then sorts and writes to a file on closing
 *
 * @author tim
 */
public class MapperRecordWriter extends RecordWriter {

    @Override
    public byte[] getOutputByteArray() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getElemCount() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    class KVP {

        public WritableComparable k;
        public WritableComparable v;

        public KVP(final WritableComparable k, final WritableComparable v) {

            this.k = k;
            this.v = v;
        }
    }
    private ByteArrayOutputStream_test[] bostream;
    private ByteArrayOutputStream_test[] ofostream;
    private DataOutputStream[][] outstream;
    private DataOutputStream[][] oostream;
    private ByteBuffer[][] offsetbuf = null;
    private ByteBuffer[][] databuf = null;
    private DynamicDirectByteBuffer[][] dyn_databuf = null;
    private DynamicDirectByteBuffer[][] dyn_offsetbuf = null;
    private byte[] byteput;
    private byte[] offsetLengthBytes;
    private byte[][] byteputArray;
    private byte[][] offsetLengthBytesArray;
    private int[] totalElements;
    private int totalPairs;
    private int offset;
    protected String str;
    protected List<WritableComparable> intList;
    private int mapID;
    private int arch;
    private int numReducers;
    private int partition;
    private Partitioner partitioner;
    private ByteQuickSort bqs;
    private int allocateLen;
    private static Properties prop;
    private int[] bytesWritten;
    private int randomHybridStream;
    Lock lock;
    AtomicIntegerArray[] fc_lock;
    AtomicIntegerArray atomicCounter;
    ReentrantLock[] reentLock;
    SpinLock[] spinLock;
    MCSLock[][] mcsLock;
    private static int hybridStreams;
    private final static Random generator;
    private String lockType;
    private boolean offheap;
    private boolean dyn_offheap;

    static {
        generator = new Random(System.currentTimeMillis());
    }

    public MapperRecordWriter(Job job, final int id, final Properties prop, DataOutputStream[][] outStream, ByteBuffer[][] databuf, DataOutputStream[][] offsetStream, ByteBuffer[][] offsetbuf, AtomicIntegerArray[] fc_lock, ReentrantLock[] reentLock, AtomicIntegerArray atomicCounter, SpinLock[] spinLock, MCSLock[][] mcsLock, String lockType) throws InstantiationException, IllegalAccessException, IOException {

        arch = Integer.parseInt(prop.getProperty("arch"));
        offheap = Boolean.parseBoolean(prop.getProperty("offheap"));
        dyn_offheap = Boolean.parseBoolean(prop.getProperty("dynamic_offheap"));
        //lock = new ReentrantLock();
        this.lockType = lockType;
        this.partitioner = job.getPartitionerClass().newInstance();

        if (job.getNumReduceTasks() == -1) {
            numReducers = Integer.parseInt(prop.getProperty("numReducerWorkers"));
        } else {
            numReducers = job.getNumReduceTasks();
        }
        if (numReducers == 0) {
            allocateLen = 1;
        } else {
            allocateLen = numReducers;
        }
        bytesWritten = new int[allocateLen];

        if (arch == 1) {

            System.err.println("Invalid architecture");
            System.exit(-1);
        } else if (arch == 2) {

            if (offheap == true) {
                if (dyn_offheap == true) {
                    this.dyn_databuf = new DynamicDirectByteBuffer[1][allocateLen];
                    this.dyn_offsetbuf = new DynamicDirectByteBuffer[1][allocateLen];
                } else {
                    this.databuf = new ByteBuffer[1][allocateLen];
                    this.offsetbuf = new ByteBuffer[1][allocateLen];
                }
            } else {
                bostream = new ByteArrayOutputStream_test[allocateLen];
                ofostream = new ByteArrayOutputStream_test[allocateLen];
                outstream = new DataOutputStream[1][allocateLen];
                oostream = new DataOutputStream[1][allocateLen];
            }

            totalElements = new int[allocateLen];

            for (int i = 0; i < allocateLen; ++i) {
                if (offheap == true) {
                    //1000000,500000
                    if (dyn_offheap == true) {
                        this.dyn_databuf[0][i] = new DynamicDirectByteBuffer(10000000);
                        this.dyn_offsetbuf[0][i] = new DynamicDirectByteBuffer(5000000);
                    } else {
                        //this.databuf[0][i] = ByteBuffer.allocateDirect(10000000);
                        //this.offsetbuf[0][i] = ByteBuffer.allocateDirect(5000000);
                        this.databuf[0][i] = ByteBuffer.allocateDirect(50000000);
                        this.offsetbuf[0][i] = ByteBuffer.allocateDirect(5000000);
                    }
                } else {
                    bostream[i] = new ByteArrayOutputStream_test();
                    ofostream[i] = new ByteArrayOutputStream_test();
                    outstream[0][i] = new DataOutputStream(bostream[i]);
                    oostream[0][i] = new DataOutputStream(ofostream[i]);
                }
                //System.out.println("here");
                //databuf[0][i] = UnsafeDirectByteBuffer.allocateAlignedByteBuffer(5000000, 8);
                //offsetbuf[0][i] = UnsafeDirectByteBuffer.allocateAlignedByteBuffer(5000000, 8);
            }
        } else if (arch == 3) {
            this.outstream = outStream;
            this.oostream = offsetStream;
            this.databuf = databuf;
            this.offsetbuf = offsetbuf;
        } else if (arch == 4) {
            hybridStreams = Integer.parseInt(prop.getProperty("hybridstreams"));
            this.outstream = outStream;
            this.oostream = offsetStream;
            this.databuf = databuf;
            this.offsetbuf = offsetbuf;
        }

        this.mapID = id;
        this.prop = prop;
        this.fc_lock = fc_lock;
        this.reentLock = reentLock;
        this.atomicCounter = atomicCounter;
        this.spinLock = spinLock;
        this.mcsLock = mcsLock;
    }

    public static String readString(final byte[] a, final int i) {

        final int len = a[i - 1] & 0x000000FF;
        return new String(Arrays.copyOfRange(a, i, i + len));
    }

    public void close(ByteBuffer buf) throws IllegalArgumentException, IllegalAccessException {
        Field cleanerField = null;
        try {
            cleanerField = buf.getClass().getDeclaredField("cleaner");
        } catch (NoSuchFieldException ex) {
            Logger.getLogger(MapperRecordWriter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SecurityException ex) {
            Logger.getLogger(MapperRecordWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
        cleanerField.setAccessible(true);
        Cleaner cleaner = (Cleaner) cleanerField.get(buf);
        cleaner.clean();
    }

    /**
     * sort it, write it
     */
    @Override
    public void close() throws IOException, InterruptedException {


        if (arch == 1) {

            System.err.println("Invalid architecture");
            System.exit(-1);

        } else if (arch == 2) {

            //flush all output streams
            byteputArray = new byte[allocateLen][];
            offsetLengthBytesArray = new byte[allocateLen][];

            for (int i = 0; i < allocateLen; ++i) {
                if (offheap == true) {
                    if (dyn_offheap == true) {
                        //byteputArray[i] = new byte[databuf[0][i].position()];
                        dyn_databuf[0][i].clear();
                        byteputArray[i] = dyn_databuf[0][i].get(byteputArray[i]);
                        //offsetLengthBytesArray[i] = new byte[offsetbuf[0][i].position()];
                        dyn_offsetbuf[0][i].clear();
                        offsetLengthBytesArray[i] = dyn_offsetbuf[0][i].get(offsetLengthBytesArray[i]);
                        totalElements[i] = dyn_offsetbuf[0][i].position();
                        //totalElements[i] = offsetLengthBytesArray[i].length;
                        dyn_databuf[0][i].destroy();
                        dyn_offsetbuf[0][i].destroy();
                        dyn_databuf[0][i] = null;
                        dyn_offsetbuf[0][i] = null;
                    } else {
                        byteputArray[i] = new byte[databuf[0][i].position()];
                        databuf[0][i].clear();
                        databuf[0][i].get(byteputArray[i]);
                        offsetLengthBytesArray[i] = new byte[offsetbuf[0][i].position()];
                        offsetbuf[0][i].clear();
                        offsetbuf[0][i].get(offsetLengthBytesArray[i]);
                        totalElements[i] = offsetbuf[0][i].position();
                        DirectByteBufferCleaner.clean(databuf[0][i]);
                        DirectByteBufferCleaner.clean(offsetbuf[0][i]);
                    }
                } else {
                    outstream[0][i].flush();
                    byteputArray[i] = bostream[i].getBuf();
                    outstream[0][i].close();
                    bostream[i] = null;
                    oostream[0][i].flush();
                    offsetLengthBytesArray[i] = ofostream[i].getBuf();
                    totalElements[i] = ofostream[i].size();
                    oostream[0][i].close();
                    ofostream[i] = null;
                    oostream[0][i] = null;
                    outstream[0][i] = null;
                }

                /*
                 * try { //clean bytebuffer close(databuf[0][i]);
                 * close(offsetbuf[0][i]); } catch (IllegalArgumentException ex)
                 * {
                 * Logger.getLogger(MapperRecordWriter.class.getName()).log(Level.SEVERE,
                 * null, ex); } catch (IllegalAccessException ex) {
                 * Logger.getLogger(MapperRecordWriter.class.getName()).log(Level.SEVERE,
                 * null, ex); }
                 */
                //System.out.println("MapId: " + this.mapID + " Reducer: " + i + " data size: " + MemoryUtil.deepMemoryUsageOf(byteputArray[i]) + "********");
            }


            //bytesWritten = null;
            //System.out.println("MaprecordWriter size: ------ " + MemoryUtil.deepMemoryUsageOf(this));
        } else if (arch == 3) {
        }

        //System.out.println("RecordWriter size: "+(MemoryUtil.deepMemoryUsageOf(this.byteputArray)+" "+MemoryUtil.deepMemoryUsageOf(this.offsetLengthBytesArray)+" "+MemoryUtil.deepMemoryUsageOf(this.totalElements)));
        //SystemMemoryUtil.printSystemMemoryStats();
//        System.out.println("MapperRecordWriter size: Total----------" + MemoryUtil.deepMemoryUsageOf(this));
//        System.out.println("MapperRecordWriter size: outstream--" + MemoryUtil.deepMemoryUsageOf(outstream));
//        System.out.println("MapperRecordWriter size: byteputArray--" + MemoryUtil.deepMemoryUsageOf(byteputArray));
//        System.out.println("MapperRecordWriter size: bostream--" + MemoryUtil.deepMemoryUsageOf(bostream));
//        System.out.println("MapperRecordWriter size: oostream--" + MemoryUtil.deepMemoryUsageOf(oostream));
//        System.out.println("MapperRecordWriter size: ofostream--" + MemoryUtil.deepMemoryUsageOf(ofostream));
//        System.out.println("MapperRecordWriter size: offsetLengthBytesArray--" + MemoryUtil.deepMemoryUsageOf(offsetLengthBytesArray) + "\n\n");
    }

    public static InputStream newInputStream(final ByteBuffer buf) {
        return new InputStream() {

            @Override
            public synchronized int read() throws IOException {
                if (!buf.hasRemaining()) {
                    return -1;
                }
                return buf.get();
            }

            @Override
            public synchronized int read(byte[] bytes, int off, int len) throws IOException {
                // Read only what's left
                len = Math.min(len, buf.remaining());
                buf.get(bytes, off, len);
                return len;
            }
        };
    }

    // This algorithm is based on the boost smart_ptr's yield_k
// algorithm.
    public void yield(int k) throws InterruptedException {
        if (k < 2) {
        } else if (k < 16) {
            Thread.sleep(0);
        } else {
            Thread.sleep(1);
        }
    }

    /**
     * writes to the file
     */
    @Override
    public void write(final WritableComparable key, final WritableComparable value) throws IOException, InterruptedException {

        if (arch == 1) {

            System.err.println("Invalid architecture");
            System.exit(-1);
        } else if (arch == 2) {

            //System.out.println("arch: " + arch);
            partition = partitioner.getPartition(key, null, numReducers);
            //System.out.println("key: " + key + " size: " + outstream[partition].size());

            if (offheap == true) {
                if (dyn_offheap == true) {
                    dyn_offsetbuf[0][partition].putInt(dyn_databuf[0][partition].position() + key.getOffset());
                    key.write(dyn_databuf[0][partition]);
                    value.write(dyn_databuf[0][partition]);
                } else {
                    offsetbuf[0][partition].putInt(databuf[0][partition].position() + key.getOffset());
                    key.write(databuf[0][partition]);
                    value.write(databuf[0][partition]);
                }
            } else {
                /**
                 * ** dataoutputstream code
                 */
                oostream[0][partition].writeInt(outstream[0][partition].size() + key.getOffset());
                bytesWritten[partition] += key.write(outstream[0][partition]);
                //System.out.println("key: "+key + " size: " + outstream[partition].size());
                bytesWritten[partition] += value.write(outstream[0][partition]);
            }
            //System.exit(0);
        } else if (arch == 3) {

            partition = partitioner.getPartition(key, null, numReducers);

            if (lockType.equals("sync")) {
                /**
                 * ****** Java synchronized ********
                 */
                if (offheap == true) {
                    synchronized (databuf[0][partition]) {
                        offsetbuf[0][partition].putInt(databuf[0][partition].position() + key.getOffset());
                        key.write(databuf[0][partition]);
                        value.write(databuf[0][partition]);
                    }
                } else {
                    synchronized (oostream[0][partition]) {
                        oostream[0][partition].writeInt(outstream[0][partition].size() + key.getOffset());
                        key.write(outstream[0][partition]);
                        value.write(outstream[0][partition]);
                    }
                }
            } else if (lockType.equals("spinlock")) {
                /**
                 * ****** SPIN Lock ********
                 */
                spinLock[partition].lock();
                try {
                    if (offheap == true) {
                        offsetbuf[0][partition].putInt(databuf[0][partition].position() + key.getOffset());
                        key.write(databuf[0][partition]);
                        value.write(databuf[0][partition]);
                    } else {
                        oostream[0][partition].writeInt(outstream[0][partition].size() + key.getOffset());
                        key.write(outstream[0][partition]);
                        value.write(outstream[0][partition]);
                    }
                } finally {
                    spinLock[partition].unlock();
                }
            } else if (lockType.equals("tas_spinlock")) {
                /**
                 * ******* TAS Lock (Test and Set) ********
                 */
                while (true) {
                    if (fc_lock[0].weakCompareAndSet(partition, 0, 1)) {
                        //if (fc_lock.compareAndSet(partition, 0, 1)) {

                        //System.out.println(this.mapID+" lock it");
                        if (offheap == true) {
                            offsetbuf[0][partition].putInt(databuf[0][partition].position() + key.getOffset());
                            key.write(databuf[0][partition]);
                            value.write(databuf[0][partition]);
                        } else {
                            oostream[0][partition].writeInt(outstream[0][partition].size() + key.getOffset());
                            key.write(outstream[0][partition]);
                            value.write(outstream[0][partition]);
                        }
                        fc_lock[0].set(partition, 0);
                        break;

                    } else {
                        Thread.yield();
                        //wait();
                    }
                }
            } else if (lockType.equals("ttas_spinlock")) /**
             * *********** TTAS Lock (Test and Test and Set) *********
             */
            {
                while (true) {
                    while (fc_lock[0].get(partition) == 1) {
                    };

                    if (fc_lock[0].weakCompareAndSet(partition, 0, 1)) {
                        if (offheap == true) {
                            offsetbuf[0][partition].putInt(databuf[0][partition].position() + key.getOffset());
                            key.write(databuf[0][partition]);
                            value.write(databuf[0][partition]);
                        } else {
                            oostream[0][partition].writeInt(outstream[0][partition].size() + key.getOffset());
                            key.write(outstream[0][partition]);
                            value.write(outstream[0][partition]);
                        }
                        fc_lock[0].set(partition, 0);
                        break;

                    } else {
                        Thread.yield();
                    }
                }
            } else if (lockType.equals("reentrantlock")) {
                /**
                 * *********** Re-Entrant Lock *********
                 */
                reentLock[partition].lock();
                try {
                    if (offheap == true) {
                        offsetbuf[0][partition].putInt(databuf[0][partition].position() + key.getOffset());
                        key.write(databuf[0][partition]);
                        value.write(databuf[0][partition]);
                    } else {
                        oostream[0][partition].writeInt(outstream[0][partition].size() + key.getOffset());
                        key.write(outstream[0][partition]);
                        value.write(outstream[0][partition]);
                    }
                } finally {
                    reentLock[partition].unlock();
                }

            } else if (lockType.equals("mcslock")) {
                mcsLock[partition][0].acquire();
                if (offheap == true) {
                    offsetbuf[0][partition].putInt(databuf[0][partition].position() + key.getOffset());
                    key.write(databuf[0][partition]);
                    value.write(databuf[0][partition]);
                } else {
                    oostream[0][partition].writeInt(outstream[0][partition].size() + key.getOffset());
                    key.write(outstream[0][partition]);
                    value.write(outstream[0][partition]);
                }
                mcsLock[partition][0].release();
            }
            /**
             * *********** Wait-Notify *********
             */
//            while (true) {
//                if (fc_lock.weakCompareAndSet(partition, 0, 1)) {
//                    oostream[partition].writeInt(outstream[partition].size() + key.getOffset());
//                    key.write(outstream[partition]);
//                    value.write(outstream[partition]);
//                    synchronized (fc_lock) {
//                        fc_lock.set(partition, 0);
//                        fc_lock.notifyAll();
//                    }
//                    break;
//
//                } else {
//                    synchronized (fc_lock) {
//                        if (fc_lock.get(partition) == 1) {
//                            fc_lock.wait();
//                        }
//                    }
//                }
//            }
            //}
        } else if (arch == 4) {

            partition = partitioner.getPartition(key, null, numReducers);
            //randomHybridStream = generator.nextInt(hybridStreams);
            //randomHybridStream = atomicCounter.incrementAndGet(partition) % hybridStreams;
            randomHybridStream = (randomHybridStream + 1) % hybridStreams;

            //outstream[randomHybridStream][partition].flush();
            //oostream[randomHybridStream][partition].flush();

            if (lockType.equalsIgnoreCase("sync")) {
                /**
                 * ****** Java synchronized ********
                 */
                if (offheap == true) {
                    synchronized (databuf[randomHybridStream][partition]) {
                        offsetbuf[randomHybridStream][partition].putInt(databuf[randomHybridStream][partition].position() + key.getOffset());
                        key.write(databuf[randomHybridStream][partition]);
                        value.write(databuf[randomHybridStream][partition]);
                    }
                } else {
                    synchronized (oostream[randomHybridStream][partition]) {
                        oostream[randomHybridStream][partition].writeInt(outstream[randomHybridStream][partition].size() + key.getOffset());
                        key.write(outstream[randomHybridStream][partition]);
                        value.write(outstream[randomHybridStream][partition]);
                    }
                }

            } else if (lockType.equalsIgnoreCase("ttas_spinlock")) {
                /**
                 * *********** TTAS Lock (Test and Test and Set) *********
                 */
                int k1 = 0, k2 = 0;
                while (true) {

                    k1 = 0;
                    while (fc_lock[randomHybridStream].get(partition) == 1) {
                        randomHybridStream = (randomHybridStream + 1) % hybridStreams;
                    };

                    if (fc_lock[randomHybridStream].weakCompareAndSet(partition, 0, 1)) {
                        if (offheap = true) {
                            offsetbuf[randomHybridStream][partition].putInt(databuf[randomHybridStream][partition].position() + key.getOffset());
                            key.write(databuf[randomHybridStream][partition]);
                            value.write(databuf[randomHybridStream][partition]);
                        } else {
                            oostream[randomHybridStream][partition].writeInt(outstream[randomHybridStream][partition].size() + key.getOffset());
                            key.write(outstream[randomHybridStream][partition]);
                            value.write(outstream[randomHybridStream][partition]);
                        }
                        fc_lock[randomHybridStream].lazySet(partition, 0);
                        break;

                    } else {
                        //randomHybridStream = generator.nextInt(hybridStreams);
                        randomHybridStream = (randomHybridStream + 1) % hybridStreams;
                        //if (k1++ > 2) {
                        Thread.yield();
                        //  k1 = 0;
                        //}
                    }
                }
            } else if (lockType.equalsIgnoreCase("reentrantlock")) {
                reentLock[partition].lock();
                try {
                    if (offheap = true) {
                        offsetbuf[randomHybridStream][partition].putInt(databuf[randomHybridStream][partition].position() + key.getOffset());
                        key.write(databuf[randomHybridStream][partition]);
                        value.write(databuf[randomHybridStream][partition]);
                    } else {
                        oostream[randomHybridStream][partition].writeInt(outstream[randomHybridStream][partition].size() + key.getOffset());
                        key.write(outstream[randomHybridStream][partition]);
                        value.write(outstream[randomHybridStream][partition]);
                    }
                } finally {
                    reentLock[partition].unlock();
                }
            } else if (lockType.equals("mcslock")) {
                mcsLock[partition][randomHybridStream].acquire();
                if (offheap == true) {
                    offsetbuf[randomHybridStream][partition].putInt(databuf[randomHybridStream][partition].position() + key.getOffset());
                    key.write(databuf[randomHybridStream][partition]);
                    value.write(databuf[randomHybridStream][partition]);
                } else {
                    oostream[randomHybridStream][partition].writeInt(outstream[randomHybridStream][partition].size() + key.getOffset());
                    key.write(outstream[randomHybridStream][partition]);
                    value.write(outstream[randomHybridStream][partition]);
                }
                mcsLock[partition][randomHybridStream].release();
            }
        }
        //System.out.println("bytes written: " + bytesWritten[partition]);

        //totalPairs++;
    }

    @Override
    public final byte[] getData() {
        return byteput;
    }

    @Override
    public final byte[][] getDataArray() {
        return byteputArray;
    }

    @Override
    public final byte[] getOffsets() {
        return offsetLengthBytes;
    }

    @Override
    public final byte[][] getOffsetsArray() {
        return offsetLengthBytesArray;
    }

    @Override
    public final int[] getElemCountArray() {
        return totalElements;
    }
}
