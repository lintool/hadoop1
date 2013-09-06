/**
 *
 */
package org.apache.hadoop.mapreduce;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.io.ByteArrayOutputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.InputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import test.hone.invertedindexing.TextPairOfIntsRecordReader;
import test.hone.kmeans.IntPairDoublePointIntNodeRecordReader;
import test.hone.mrlda.PairOfIntsDoubleRecordReader;
import test.hone.pagerank.IntPageRankNodeRecordReader;
import static vanilla.java.affinity.AffinityStrategies.*;
import vanilla.java.affinity.AffinityThreadFactory;

/**
 * The actual Map Reduce implementation This is a very simple implementation
 * that launches multiple threads for the Map and then performs and
 * multithreaded sort of the Map output before firing at a single Reducer at the
 * moment. In the future this will support multiple reduce operations in
 * parallel.
 *
 * @author tim
 */
public class JobExecutor {

    protected int submissionCount = 0;
    protected Job job;
    // TODO CONFIGURIFY all this ja 
    // 1 meg chunks
    protected static long chunkSize = 1024 * 1024;
    // 100k chunk
    protected static char lineTerminator = '\n';  // uhmmm...
    private Properties prop;
    private int totalMapExecuted = 0;
    private org.apache.hadoop.io.ByteArrayOutputStream[] bostream;
    private org.apache.hadoop.io.ByteArrayOutputStream[] ofostream;

    /**
     * @param job to submit
     * @param inCurrentThread true if to run in this thread, false if to spawn
     * and return immediately
     */
    public final void execute(final Job job, final boolean inCurrentThread) {
        this.job = job;
        run();
//        if (inCurrentThread) {
//            run();
//        } else {
//            final Thread t = new Thread(this);
//            t.start();
//        }
    }

    public Object[] splitSortStreams1(byte[] dataByteArray, byte[] offsetByteArray) throws InstantiationException, IllegalAccessException, IOException {
        int singleSplitSize = Integer.parseInt(prop.getProperty("singleSortSplitSize"));
        int noSplits = (offsetByteArray.length / 4) / singleSplitSize;
        //int noSplits = 5;
        Object[] splitStreams = new Object[noSplits * 2 + 2];
        int splitSize = (offsetByteArray.length / 4) / noSplits;

        WritableComparable key = job.getMapOutputKeyClass().newInstance();
        WritableComparable value = job.getMapOutputValueClass().newInstance();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(dataByteArray));
        //DataInputStream offsetDis = new DataInputStream(new ByteArrayInputStream(offsetByteArray));

        ByteArrayOutputStream_test bostream = new ByteArrayOutputStream_test();
        DataOutputStream dos = new DataOutputStream(bostream);
        ByteArrayOutputStream_test offsetBostream = new ByteArrayOutputStream_test();
        DataOutputStream offsetDos = new DataOutputStream(offsetBostream);

        int ind = 0;
        int split = 0;
        while (dis.available() > 0) {
            dos.flush();
            offsetDos.writeInt(dos.size() + key.getOffset());
            key.readFields(dis);
            value.readFields(dis);
            key.write(dos);
            value.write(dos);
            //offset.readFields(offsetDis);
            //offset.write(offsetDos);
            ind++;
            if (ind == splitSize) {
                ind = 0;
                dos.flush();
                bostream.flush();
                offsetDos.flush();
                offsetBostream.flush();
                splitStreams[split++] = bostream.toByteArray();
                splitStreams[split++] = offsetBostream.toByteArray();
                dos.close();
                dos = null;
                bostream.close();
                bostream = null;
                offsetDos.close();
                offsetDos = null;
                offsetBostream.close();
                offsetBostream = null;
                bostream = new ByteArrayOutputStream_test();
                dos = new DataOutputStream(bostream);
                offsetBostream = new ByteArrayOutputStream_test();
                offsetDos = new DataOutputStream(offsetBostream);
            }
        }

        if (dos != null) {
            dos.flush();
            bostream.flush();
            offsetDos.flush();
            offsetBostream.flush();
            splitStreams[split++] = bostream.toByteArray();
            splitStreams[split++] = offsetBostream.toByteArray();
            dos.close();
            dos = null;
            bostream.close();
            bostream = null;
            offsetDos.close();
            offsetDos = null;
            offsetBostream.close();
            offsetBostream = null;
        }

        return splitStreams;
    }

    public Object[] splitSortStreams(byte[] dataByteArray, byte[] offsetByteArray, int totalElements) throws InstantiationException, IllegalAccessException, IOException {
        int singleSplitSize = Integer.parseInt(prop.getProperty("singleSortSplitSize"));
        if (totalElements > offsetByteArray.length) {
            totalElements = offsetByteArray.length;
        }

        int noSplits = (totalElements / 4) / singleSplitSize;
        //int noSplits = 5;
        Object[] splitStreams = new Object[noSplits * 2 + 2];

        int splitSize;
        if (noSplits != 0) {
            splitSize = (totalElements / 4) / noSplits;
        }

        WritableComparable key = job.getMapOutputKeyClass().newInstance();

        int offsetSplitSize, firstSplitEleIndex = 0, split = 0, firstSplitOffsetEleIndex = 0;
        byte[] repairOffsetStream;
        for (int i = 0; i < noSplits; ++i) {

            offsetSplitSize = (singleSplitSize * (i + 1)) * 4;
            int dataArrayLastEleIndex = ByteUtil.readInt(offsetByteArray, offsetSplitSize);
            dataArrayLastEleIndex -= key.getOffset();
            if (dataArrayLastEleIndex > dataByteArray.length || dataArrayLastEleIndex < 0) {
                System.out.println("out of bound error --> " + dataByteArray.length + " " + offsetByteArray.length + " " + dataArrayLastEleIndex);
            }

            splitStreams[split++] = Arrays.copyOfRange(dataByteArray, firstSplitEleIndex, dataArrayLastEleIndex);
            if (i == 0) {
                splitStreams[split++] = Arrays.copyOfRange(offsetByteArray, firstSplitOffsetEleIndex, offsetSplitSize);
            } else {

                repairOffsetStream = Arrays.copyOfRange(offsetByteArray, firstSplitOffsetEleIndex, offsetSplitSize);
                int offsetValue = ByteUtil.readInt(repairOffsetStream, 0);
                int subtractValue = offsetValue - key.getOffset();
                for (int j = 0; j < repairOffsetStream.length / 4; ++j) {

                    offsetValue = ByteUtil.readInt(repairOffsetStream, j * 4);
                    offsetValue -= subtractValue;
                    ByteUtil.writeInt(repairOffsetStream, j * 4, offsetValue);
                }

                splitStreams[split++] = repairOffsetStream;
            }

            firstSplitEleIndex = dataArrayLastEleIndex;
            firstSplitOffsetEleIndex = offsetSplitSize;
        }

        if (noSplits * 4 * singleSplitSize < totalElements) {

            splitStreams[split++] = Arrays.copyOfRange(dataByteArray, firstSplitEleIndex, dataByteArray.length);
            repairOffsetStream = Arrays.copyOfRange(offsetByteArray, firstSplitOffsetEleIndex, totalElements);
            int offsetValue = ByteUtil.readInt(repairOffsetStream, 0);
            int subtractValue = offsetValue - key.getOffset();
            for (int j = 0; j < repairOffsetStream.length / 4; ++j) {

                offsetValue = ByteUtil.readInt(repairOffsetStream, j * 4);
                offsetValue -= subtractValue;
                ByteUtil.writeInt(repairOffsetStream, j * 4, offsetValue);
            }

            splitStreams[split++] = repairOffsetStream;
        }

        return splitStreams;
    }

    /**
     * The real Map Reduce functionality Currently this will launch a single
     * reduce function only
     */
    @SuppressWarnings("unchecked")
    public final void run() {

        try {
            prop = new Properties();
            prop.load(new FileInputStream("config.properties"));
            final File tempDirectory = new File(prop.getProperty("ramdisk_path"));
            final int numMapperWorkers = job.getNumMapTasks();
            final int numReducerWorkers = (job.getNumReduceTasks() == 0) ? 1 : job.getNumReduceTasks();
            final int numMapperWorkerPool = Integer.parseInt(prop.getProperty("numMapperWorkerPool"));
            final int numReducerWorkerPool = Integer.parseInt(prop.getProperty("numReducerWorkerPool"));
            final int numSortWorkerPool = Integer.parseInt(prop.getProperty("numSortWorkerPool"));
            final int arch = Integer.parseInt(prop.getProperty("arch"));
            final int splitDecision = Integer.parseInt(prop.getProperty("splitDecision"));
            final int forkjoinpool = Integer.parseInt(prop.getProperty("forkjoinpool"));
            final String lockType = prop.getProperty("lockType");
            final boolean offheap = Boolean.parseBoolean(prop.getProperty("offheap"));
            final Partitioner partitioner = job.getPartitionerClass().newInstance();
            ThreadPoolExecutor mapExecutorServiceThread = null, sortExecutorServiceThread = null, reducerExecutorServiceThread = null;
            ForkJoinPool mapExecutorServiceFork = null, sortExecutorServiceFork = null, reducerExecutorServiceFork = null;

            //final ThreadPoolExecutor mapExecutorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(numMapperWorkerPool);
            if (forkjoinpool == 0) {
                mapExecutorServiceThread = (ThreadPoolExecutor) Executors.newFixedThreadPool(numMapperWorkerPool, new AffinityThreadFactory("bg", SAME_CORE, SAME_SOCKET, ANY));
            } else {
                mapExecutorServiceFork = new ForkJoinPool(numMapperWorkerPool, ForkJoinPool.defaultForkJoinWorkerThreadFactory, Thread.getDefaultUncaughtExceptionHandler(), true);
            }

            final List<Callable<Object>> maptasks = new ArrayList<Callable<Object>>();

            int mapperCount = 0;
            int id = 0;

            List<Future< Object>> byteBufferList = null;
            final long start = System.currentTimeMillis();
            final long start1 = System.currentTimeMillis();

            Object[] intermArray = null;
            Object[] intermArray1 = null;

            Object[] returnData = new Object[3];
            byte[][] byteputArray;
            byte[][] offsetArray;

            int hybridStreams = Integer.parseInt(prop.getProperty("hybridstreams"));
            ArrayList<Future<Object>> set = new ArrayList();
            ByteArrayOutputStream_long[][] bostream = null;
            ByteArrayOutputStream_long[][] ofostream = null;
            //FileBackedOutputStream[][] bostream = null;
            //FileBackedOutputStream[][] ofostream = null;

            DataOutputStream[][] outstream = null;
            DataOutputStream[][] oostream = null;
            ByteBuffer[][] databuf = null;
            ByteBuffer[][] offsetbuf = null;
            int[][] totalElementsArray = null;

            AtomicIntegerArray atomicCounter = new AtomicIntegerArray(numReducerWorkers);
            ReentrantLock[] reentLock = null;
            SpinLock[] spinLock = null;
            AtomicIntegerArray[] fc_lock = null;
            MCSLock[][] mcsLock = null;

            if (arch == 3 || arch == 4) {
                if (lockType.equals("reentrantlock")) {
                    reentLock = new ReentrantLock[numReducerWorkers];
                    for (int i = 0; i < numReducerWorkers; ++i) {
                        reentLock[i] = new ReentrantLock();
                    }
                } else if (lockType.equals("spinlock")) {
                    spinLock = new SpinLock[numReducerWorkers];
                    for (int i = 0; i < numReducerWorkers; ++i) {
                        spinLock[i] = new SpinLock();
                    }
                } else if (lockType.contains("tas")) {
                    fc_lock = new AtomicIntegerArray[hybridStreams];
                    for (int i = 0; i < hybridStreams; ++i) {
                        fc_lock[i] = new AtomicIntegerArray(numReducerWorkers);
                    }
                    for (int j = 0; j < hybridStreams; ++j) {
                        for (int i = 0; i < numReducerWorkers; ++i) {
                            fc_lock[j].set(i, 0);
                        }
                    }
                } else if (lockType.contains("mcslock")) {
                    mcsLock = new MCSLock[numReducerWorkers][hybridStreams];
                    for (int i = 0; i < numReducerWorkers; ++i) {
                        for (int j = 0; j < hybridStreams; ++j) {
                            mcsLock[i][j] = new MCSLock();
                        }
                    }
                }
            }

            if (arch != 4) {
                hybridStreams = 1;
            }

            if (arch == 3 || arch == 4) {
                //bostream = new FileBackedOutputStream[hybridStreams][numReducerWorkers];
                //ofostream = new FileBackedOutputStream[hybridStreams][numReducerWorkers];

                //outstream = new DataOutputStream[hybridStreams][numReducerWorkers];
                //oostream = new DataOutputStream[hybridStreams][numReducerWorkers];
                totalElementsArray = new int[hybridStreams][numReducerWorkers];
                if (offheap == true) {
                    databuf = new ByteBuffer[hybridStreams][numReducerWorkers];
                    offsetbuf = new ByteBuffer[hybridStreams][numReducerWorkers];
                } else {
                    bostream = new ByteArrayOutputStream_long[hybridStreams][numReducerWorkers];
                    ofostream = new ByteArrayOutputStream_long[hybridStreams][numReducerWorkers];
                    outstream = new DataOutputStream[hybridStreams][numReducerWorkers];
                    oostream = new DataOutputStream[hybridStreams][numReducerWorkers];
                }

                for (int j = 0; j < hybridStreams; ++j) {
                    for (int i = 0; i < numReducerWorkers; ++i) {
                        if (offheap == true) {
                            databuf[j][i] = ByteBuffer.allocateDirect(1000000);
                            offsetbuf[j][i] = ByteBuffer.allocateDirect(500000);
                        } else {
                            bostream[j][i] = new ByteArrayOutputStream_long(1024 * 1024);
                            ofostream[j][i] = new ByteArrayOutputStream_long(1024 * 1024);
                            outstream[j][i] = new DataOutputStream(bostream[j][i]);
                            oostream[j][i] = new DataOutputStream(ofostream[j][i]);
                        }
                    }
                }
            }

            byteputArray = new byte[numReducerWorkers * hybridStreams][];
            offsetArray = new byte[numReducerWorkers * hybridStreams][];

            if ((job.getFirstIterationInputFromDisk() == true && job.getInMemoryInput() == true && job.getIterationNo() == 0) || job.getInMemoryInput() == false) {

                final InputFormat inputFormat1 = job.getInputFormat().newInstance();
                final LoadFilesToMemory loadToMemory = new LoadFilesToMemory(inputFormat1);
                byteBufferList = loadToMemory.getByteBuffers(job.getInputDirectoryPath());
                totalMapExecuted = LoadFilesToMemory.getNoOfSplits();
                job.setNumMapTasks(totalMapExecuted);

                if (arch == 2) {
                    intermArray = new Object[totalMapExecuted * 3];
                } else if (arch == 3 || arch == 4) {
                    intermArray = new Object[1 * 3];
                }

                //System.out.println("Load size: " + MemoryUtil.deepMemoryUsageOf(byteBufferList));
            } else if (job.getInMemoryInput() == true) {
                if (job.getMapInputFromReducer() == true) {
                    byteBufferList = job.getMapInput();
                }

                totalMapExecuted = job.getNumMapTasks();

                if (arch == 2) {
                    intermArray = new Object[totalMapExecuted * 3];
                } else if (arch == 3 || arch == 4) {
                    intermArray = new Object[1 * 3];
                }

                if (byteBufferList == null) {
                    ArrayList list = job.getVariableList("map");
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    byte[] sequenceBytes;
                    int count = totalMapExecuted;
                    int listSize = list.size();
                    int iters = listSize / count;
                    int ind = 0;

                    for (Object ele : list) {
                        ByteBuffer byteBuffer = (ByteBuffer) ele;
                        if (byteBuffer != null) {
                            id++;
                        }
                    }

                    intermArray = new Object[id][3];

                    id = 0;
                    for (Object ele : list) {
                        ByteBuffer byteBuffer = (ByteBuffer) ele;
                        if (byteBuffer != null) {
                            final InputFormat inputFormat = job.getInputFormat().newInstance();
                            inputFormat.initialize(job);

                            final Mapper mapper = job.getMapperClass().newInstance();

                            final RecordWriter writer = new MapperRecordWriter(job, id, prop, outstream, databuf, oostream, offsetbuf, fc_lock, reentLock, atomicCounter, spinLock, mcsLock, lockType);

                            final org.apache.hadoop.mapreduce.Mapper.Context mapContext = mapper.new Context(inputFormat, writer, byteBuffer, job);

                            CallableMapper calMapper = new CallableMapper(mapper, mapContext, id++, prop, intermArray);
                            // maptasks.add(calMapper);

                            Future future;
                            if (forkjoinpool == 1) {
                                future = mapExecutorServiceFork.submit(calMapper);
                            } else {
                                future = mapExecutorServiceThread.submit(calMapper);
                            }
                            set.add(future);
                        } else {
                            break;
                        }

                    }
                    job.remVariableList("map");
                }
            }

            // final int numOfBuffers = byteBufferList.size();
            System.out.println("Done 2");

            if (byteBufferList != null) {

                for (Iterator<Future<Object>> i = byteBufferList.iterator(); i.hasNext(); i.next()) {
                    id++;
                }

                if (id == 0) {
                    id = 1;
                }
                intermArray = new Object[id * 3];

                id = 0;
                for (Iterator<Future<Object>> i = byteBufferList.iterator(); i.hasNext();) {
                    ByteBuffer byteBuffer = (ByteBuffer) (i.next()).get();
                    i.remove();
                    //(ByteBuffer) fut.get();

                    System.out.println("Added");

                    final InputFormat inputFormat = job.getInputFormat().newInstance();
                    inputFormat.initialize(job);

                    final Mapper mapper = job.getMapperClass().newInstance();

                    final RecordWriter writer = new MapperRecordWriter(job, id, prop, outstream, databuf, oostream, offsetbuf, fc_lock, reentLock, atomicCounter, spinLock, mcsLock, lockType);

                    final org.apache.hadoop.mapreduce.Mapper.Context mapContext = mapper.new Context(inputFormat, writer, byteBuffer, job);

                    //maptasks.add(new CallableMapper(mapper, mapContext, id++, prop, intermArray));
                    CallableMapper calMapper = new CallableMapper(mapper, mapContext, id++, prop, intermArray);

                    Future future;
                    if (forkjoinpool == 1) {
                        future = mapExecutorServiceFork.submit(calMapper);
                    } else {
                        future = mapExecutorServiceThread.submit(calMapper);
                    }
                    set.add(future);
                }
            }

            mapperCount = id;
            totalMapExecuted = id;
            //long noBytes = MemoryUtil.deepMemoryUsageOf(byteBufferList);

            //System.out.println("******* Byte buffer list Size ********: " + noBytes);
            if (byteBufferList != null) {
                byteBufferList.clear();
            }
            //noBytes = MemoryUtil.deepMemoryUsageOf(maptasks);

            //System.out.println("******* maptasks list Size ********: " + noBytes);
            //byteBufferList.clear();

            System.out.println("Mapper count: " + mapperCount);

            System.out.println("Wait for the maps to finish");
            //mapExecutorService.invokeAll(maptasks);
            for (Iterator<Future<Object>> i = set.iterator();
                    i.hasNext();) {
                (i.next()).get();
                i.remove();
            }

//            for (Future<Object> future : set) {
//                future.get();
//            }
            if (mapExecutorServiceThread != null) {
                mapExecutorServiceThread.shutdown();
            } else {
                mapExecutorServiceFork.shutdown();
            }

            final long end1 = System.currentTimeMillis();
            System.out.println("Map stage finished");
            System.out.println("Map Stage Execution time was " + (end1 - start1) + " ms.");

            if (arch == 3 || arch == 4) {
                int ind = 0;
                for (int i = 0; i < numReducerWorkers; ++i) {
                    for (int j = 0; j < hybridStreams; ++j) {
                        if (offheap == true) {
                            byteputArray[ind] = new byte[databuf[j][i].position()];
                            databuf[j][i].clear();
                            databuf[j][i].get(byteputArray[ind]);
                            offsetArray[ind] = new byte[offsetbuf[j][i].position()];
                            offsetbuf[j][i].clear();
                            offsetbuf[j][i].get(offsetArray[ind]);
                            totalElementsArray[j][i] = offsetbuf[j][i].position();
                            ind++;
                        } else {
                            outstream[j][i].flush();
                            outstream[j][i].close();
                            byteputArray[ind] = bostream[j][i].toByteArray();
                            //System.out.println("Reducer: " + i + " data size: " + MemoryUtil.deepMemoryUsageOf(byteputArray[ind]) + "********");
                            oostream[j][i].flush();
                            oostream[j][i].close();

                            offsetArray[ind++] = ofostream[j][i].toByteArray();

                            outstream[j][i] = null;
                            bostream[j][i] = null;
                            oostream[j][i] = null;
                            ofostream[j][i] = null;
                            //totalElementsArray[j][i] = offsetArray.length;
                        }
                    }
                }

//                for (int j = 0; j < hybridStreams; ++j) {
//                    for (int i = 0; i < numReducerWorkers; ++i) {
//                        outstream[j][i] = null;
//                        bostream[j][i] = null;
//                        oostream[j][i] = null;
//                        ofostream[j][i] = null;
//                    }
//                    outstream[j] = null;
//                    bostream[j] = null;
//                    oostream[j] = null;
//                    ofostream[j] = null;
//                }

                //returnData[0] = byteputArray;
                //returnData[1] = offsetArray;
                //intermArray[0] = returnData;
                intermArray[0] = byteputArray;
                intermArray[1] = offsetArray;
                intermArray[2] = totalElementsArray;

            }

            //SystemMemoryUtil.printSystemMemoryStats();
            //noBytes = MemoryUtil.deepMemoryUsageOf(mapExecutorService);

            //System.out.println("******* Map Executor Service Object Size ********: " + noBytes);

            //long noBytes = MemoryUtil.deepMemoryUsageOf(invokeAll);
            //System.out.println("******* Intermediate Output Size ********: " + noBytes);
            //noBytes = MemoryUtil.deepMemoryUsageOf(intermArray);

            //System.out.println("******* Intermediate-array Output Size ********: " + noBytes);

            //final ThreadPoolExecutor reducerExecutorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(numReducerWorkerPool);

            final List<Callable<Object>> reducetasks = new ArrayList<Callable<Object>>();
            final List<Callable<Object>> sorttasks = new ArrayList<Callable<Object>>();
            final List<Callable<Object>> outputConvertTasks = new ArrayList<Callable<Object>>();

            System.out.println("Starting the Reduce stage");

            List<Future<Object>> invokeAll1 = null;

            set.clear();
            int sortCount = 0;

            int[] extraNoSplits = null;
            int totalSortStreams = 0;
            if (arch == 2 || arch == 3 || arch == 4) {

                if (arch == 2) {
                    intermArray1 = new Object[totalMapExecuted * numReducerWorkers * 3];
                } else if (arch == 3 || arch == 4) {
                    intermArray1 = new Object[totalMapExecuted * numReducerWorkers * 1000 * 3];
                }

                long start2 = System.currentTimeMillis();
//                final ThreadPoolExecutor sortExecutorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(numSortWorkerPool);
                if (forkjoinpool == 0) {
                    sortExecutorServiceThread = (ThreadPoolExecutor) Executors.newFixedThreadPool(numSortWorkerPool, new AffinityThreadFactory("bg", SAME_CORE, SAME_SOCKET, ANY));
                } else {
                    sortExecutorServiceFork = new ForkJoinPool(numSortWorkerPool, ForkJoinPool.defaultForkJoinWorkerThreadFactory, Thread.getDefaultUncaughtExceptionHandler(), true);
                }


                int defaultNumRed = 0;
                if (numReducerWorkers == 0) {
                    defaultNumRed = 1;
                } else {
                    defaultNumRed = numReducerWorkers;
                }

                SortStreams[] sortStreamObj;
                extraNoSplits = new int[defaultNumRed * hybridStreams];
                int ind = 0;
                if (arch != 4 && arch != 3) {
                    sortStreamObj = new SortStreams[defaultNumRed * mapperCount];
                    for (int j = 0; j < defaultNumRed; ++j) {
                        for (int i = 0; i < mapperCount; ++i) {

                            //if (intermArray[i][0] != null) {
                            if (intermArray[i * 3 + 0] != null) {

                                //final Object[] intermediateDataStream = intermArray[i];
//                                if (intermediateDataStream == null) {
//                                    System.out.println("J: " + j + ", NULL");
//                                }
                                final byte[] intermediateData = ((byte[][]) intermArray[i * 3 + 0])[j];
                                final byte[] intermediateDataOffsets = ((byte[][]) intermArray[i * 3 + 1])[j];
                                final int intermediateDataOffsetsSize = (int) ((int[]) intermArray[i * 3 + 2])[j];
                                //final byte[] intermediateData = ((byte[][]) intermediateDataStream[0])[j];
                                //final byte[] intermediateDataOffsets = ((byte[][]) intermediateDataStream[1])[j];
                                int totalElements = 0;
                                if (intermediateData != null) {
                                        totalElements = intermediateDataOffsetsSize;                                    
                                }

//                                if (intermediateData != null) {
//                                    totalElements = ((int[]) intermArray[i * 2 + 2])[j];
//                                } else {
//                                    totalElements = intermediateDataOffsets.length;
//                                }

//                                if (intermediateDataStream[2] != null) {
//                                    totalElements = ((int[]) intermediateDataStream[2])[j];
//                                } else {
//                                    totalElements = intermediateDataOffsets.length;
//                                }
                                sortStreamObj[ind] = new SortStreams(intermediateData, intermediateDataOffsets, totalElements, job, intermArray1, sortCount++);
                                ind++;
                            }
                        }
                    }
                    totalSortStreams = ind;
                } else {
                    sortStreamObj = new SortStreams[defaultNumRed * mapperCount * 1000];
                    //final Object[] intermediateDataStream = intermArray[0];
                    ind = 0;
                    int ind1 = 0, ind2 = 0;
                    for (int i = 0; i < defaultNumRed * hybridStreams; ++i) {

                        final int intermediateDataOffsetsSize, totalElements;
                        final byte[] intermediateData = ((byte[][]) intermArray[0 * 3 + 0])[i];
                        final byte[] intermediateDataOffsets = ((byte[][]) intermArray[0 * 3 + 1])[i];

                        if (offheap == true) {
                            intermediateDataOffsetsSize = (int) ((int[][]) intermArray[0 * 3 + 2])[ind1][ind2];
                            totalElements = intermediateDataOffsetsSize;
                        } else {
                            totalElements = intermediateDataOffsets.length;
                        }

                        if (ind2++ == defaultNumRed - 1) {
                            ind1++;
                            ind2 = 0;
                        }
//                        final byte[] intermediateData = ((byte[][]) intermediateDataStream[0])[i];
//                        final byte[] intermediateDataOffsets = ((byte[][]) intermediateDataStream[1])[i];
                        int noSplits;
                        if (splitDecision == 1) {

                            Object[] splitStreams = splitSortStreams(intermediateData, intermediateDataOffsets, totalElements);
                            noSplits = 0;
                            for (int j = 0; j < splitStreams.length / 2; ++j) {
                                //System.out.println(defaultNumRed + " " + mapperCount + " " + ind + " " + splitStreams.length / 2 + " " + 2 * j + " " + (2 * j + 1));
                                if (splitStreams[2 * j] != null) {
                                    sortStreamObj[ind++] = new SortStreams((byte[]) splitStreams[2 * j], (byte[]) splitStreams[2 * j + 1], ((byte[]) splitStreams[2 * j + 1]).length, job, intermArray1, sortCount++);
                                    noSplits++;
                                }
                            }

                        } else {
                            sortStreamObj[ind++] = new SortStreams(intermediateData, intermediateDataOffsets, totalElements, job, intermArray1, sortCount++);
                            noSplits = 1;
                        }
                        extraNoSplits[i] = noSplits;
                    }
                    totalSortStreams = ind;
                }

                start2 = System.currentTimeMillis();
                for (int i = 0; i < totalSortStreams; ++i) {
                    Future future;
                    if (forkjoinpool == 1) {
                        future = sortExecutorServiceFork.submit(sortStreamObj[i]);
                    } else {
                        future = sortExecutorServiceThread.submit(sortStreamObj[i]);
                    }
                    set.add(future);
                }

//                intermArray = null;
//                returnData[0] = null;
//                returnData[1] = null;
//                returnData = null;
////                for (int i = 0; i < numReducerWorkers * hybridStreams; ++i) {
////                    byteputArray[i] = null;
////                    offsetArray[i] = null;
////                }
//                byteputArray = null;
//                offsetArray = null;

                System.out.println("Sort count: " + sortCount);
                //System.gc();

                //invokeAll1 = sortExecutorService.invokeAll(sorttasks);
                for (Iterator<Future<Object>> i = set.iterator(); i.hasNext();) {
                    (i.next()).get();
                    i.remove();
                }

                if (sortExecutorServiceThread != null) {
                    sortExecutorServiceThread.shutdown();
                } else {
                    sortExecutorServiceFork.shutdown();
                }
//                for (int i = 0; i < totalSortStreams; ++i) {
//                    sortStreamObj[i] = null;
//                }

                //System.gc();
                //SystemMemoryUtil.printSystemMemoryStats();
                final long end2 = System.currentTimeMillis();
                System.out.println("Sort Execution time was " + (end2 - start2) + " ms.");

                //noBytes = MemoryUtil.deepMemoryUsageOf(intermArray1);
                //System.out.println("******* Sort Output Size ********: " + noBytes);

            }

            //Combiner support start
            List<Future<Object>> combinerOutput = null;
            Object[][] intermArray2;
            Object[] reducerInput;

            set.clear();

            if (job.getCombinerClass() != null) {
                intermArray2 = new Object[250000][3];
                final List<Callable<Object>> combinertasks = new ArrayList<Callable<Object>>();
                //final ThreadPoolExecutor combinerExecutorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(numReducerWorkerPool);
                final ThreadPoolExecutor combinerExecutorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(numReducerWorkerPool, new AffinityThreadFactory("bg", SAME_CORE, SAME_SOCKET, ANY));

                //for (Future< Object> fut : invokeAll1) {
                for (int i = 0; i < sortCount; ++i) {
                    final List<RecordReader> readers = new LinkedList<RecordReader>();
                    final Object[] intermediateDataStream = (Object[]) intermArray1[i];
                    //(Object[]) fut.get();
                    final byte[] intermediateData = ((byte[]) intermediateDataStream[0]);
                    final byte[] intermediateDataOffsets = ((byte[]) intermediateDataStream[1]);
                    final int intermediateDataOffsetsSize = ((int) intermediateDataStream[2]);
                    final boolean add = readers.add(initialiseReader(intermediateData, intermediateDataOffsets, intermediateDataOffsetsSize));

                    final KeyValuesIterator kvi = new KeyValuesIterator(readers, prop);
                    OutputFormat outputFormat = job.getOutputFormat().newInstance();

                    final Combiner combiner = job.getCombinerClass().newInstance();

                    // start the reduce
                    final org.apache.hadoop.mapreduce.Combiner.Context combinerContext = combiner.new Context(kvi, outputFormat.getRecordWriter(job), job.getConfiguration());
                    //combinertasks.add(new CallableCombiner(combiner, combinerContext, prop, intermArray, i+1));
                    Future future = combinerExecutorService.submit(new CallableCombiner(combiner, combinerContext, prop, intermArray2, i + 1));
                    set.add(future);
                }
                //combinerOutput = combinerExecutorService.invokeAll(combinertasks);
                for (Iterator<Future<Object>> i = set.iterator(); i.hasNext();) {
                    (i.next()).get();
                    i.remove();
                }
                combinerExecutorService.shutdown();

                //noBytes = MemoryUtil.deepMemoryUsageOf(intermArray2);
                //System.out.println("******* Combiner Output Size ********: " + noBytes);

                //SystemMemoryUtil.printSystemMemoryStats();
                reducerInput = intermArray2;
                intermArray1 = null;
                //System.gc();

            } else {
                combinerOutput = invokeAll1;
                reducerInput = intermArray1;
                intermArray2 = null;
            }


            /**
             * ***** Start Reducer Phase ********
             */
            if (forkjoinpool == 0) {
                reducerExecutorServiceThread = (ThreadPoolExecutor) Executors.newFixedThreadPool(numReducerWorkerPool, new AffinityThreadFactory("bg", SAME_CORE, SAME_SOCKET, ANY));
            } else {
                reducerExecutorServiceFork = new ForkJoinPool(numReducerWorkerPool, ForkJoinPool.defaultForkJoinWorkerThreadFactory, Thread.getDefaultUncaughtExceptionHandler(), true);
            }

            int ind = 0;
            int reducerId = 0;
            int noIterations = 0;
            int current_split = 0;
            List<RecordReader> readers = new LinkedList<RecordReader>();
            if (arch == 2) {
                noIterations = mapperCount * numReducerWorkers;
            } else if (arch == 5) {
                noIterations = numReducerWorkers;
            } else if (arch == 4 || arch == 3) {
                //noIterations = numReducerWorkers * hybridStreams;
                noIterations = totalSortStreams;
            }

            if (job.getNumReduceTasks() == 0) {
                noIterations = 0;
            }

            for (int i = 0; i < noIterations; ++i) {
                //final Object[] intermediateDataStream = reducerInput[i].clone();
                //final byte[] intermediateData = ((byte[]) intermediateDataStream[0]);
                //final byte[] intermediateDataOffsets = ((byte[]) intermediateDataStream[1]);
                final byte[] intermediateData = (byte[]) reducerInput[i * 3 + 0];
                final byte[] intermediateDataOffsets = (byte[]) reducerInput[i * 3 + 1];
                final int intermediateDataOffsetsSize = ((int) reducerInput[i * 3 + 2]);
                final boolean add = readers.add(initialiseReader(intermediateData, intermediateDataOffsets, intermediateDataOffsetsSize));
                reducerInput[i] = null;
                final Reducer reducer = job.getReducerClass().newInstance();

                if (arch == 2) {
                    if (readers.size() == mapperCount) {
                        reducer.setParams(reducerId++, partitioner, numReducerWorkers);
                        final KeyValuesIterator kvi = new KeyValuesIterator(readers, prop);
                        OutputFormat outputFormat = job.getOutputFormat().newInstance();
                        final org.apache.hadoop.mapreduce.Reducer.Context reducerContext = reducer.new Context(kvi, outputFormat.getRecordWriter(job), job);
                        reducetasks.add(new CallableReducer(reducer, reducerContext, prop));
                        readers = new LinkedList<RecordReader>();
                    }
                } else if (arch == 5) {

                    reducer.setParams(i, partitioner, numReducerWorkers);
                    final KeyValuesIterator kvi = new KeyValuesIterator(readers, prop);
                    OutputFormat outputFormat = job.getOutputFormat().newInstance();
                    final org.apache.hadoop.mapreduce.Reducer.Context reducerContext = reducer.new Context(kvi, outputFormat.getRecordWriter(job), job);
                    reducetasks.add(new CallableReducer(reducer, reducerContext, prop));
                    readers = new LinkedList<RecordReader>();
                } else if (arch == 4 || arch == 3) {

                    if (readers.size() == extraNoSplits[current_split] * hybridStreams) {
                        reducer.setParams(reducerId++, partitioner, numReducerWorkers);
                        final KeyValuesIterator kvi = new KeyValuesIterator(readers, prop);
                        OutputFormat outputFormat = job.getOutputFormat().newInstance();
                        final org.apache.hadoop.mapreduce.Reducer.Context reducerContext = reducer.new Context(kvi, outputFormat.getRecordWriter(job), job);
                        reducetasks.add(new CallableReducer(reducer, reducerContext, prop));
                        readers = new LinkedList<RecordReader>();
                    }

                    ind++;
                    if (extraNoSplits[current_split] == ind) {
                        ind = 0;
                        current_split++;
                    }
                } else {

                    System.err.println("Invalid architecture");
                    System.exit(-1);
                }
            }

            //System.gc();
            System.out.println("Start invoking reduce stage");
            List<Future<Object>> reduceOutput = null;
            final long start3 = System.currentTimeMillis();
            if (forkjoinpool == 1) {
                reduceOutput = reducerExecutorServiceFork.invokeAll(reducetasks);
                reducerExecutorServiceFork.shutdown();
            } else {
                reduceOutput = reducerExecutorServiceThread.invokeAll(reducetasks);
                reducerExecutorServiceThread.shutdown();
            }
            final long end = System.currentTimeMillis();

            //SystemMemoryUtil.printSystemMemoryStats();

            job.setReduceOutput(reduceOutput);
            //noBytes = MemoryUtil.deepMemoryUsageOf(reduceOutput);

            //System.out.println("******* Reducer Output Size ********: " + noBytes);


            System.out.println("Reduce Stage Execution time was " + (end - start3) + " ms.");
            System.out.println("Execution time was " + (end - start) + " ms.");

            mapperCount = totalSortStreams;
            if ((job.getNumReduceTasks() == 0 || job.getNumReduceTasks() == -1) && job.getIterationOutputToDisk() == true && job.getIterationNo() == job.getEndIter()) {
                int i = 0;
                FileOutputStream fos;
                for (int j = 0; j < mapperCount; ++j) {
                    fos = new FileOutputStream(new File(job.getOutputFile().getAbsolutePath() + (i++)));
                    //final Object[] intermediateDataStream = reducerInput[j];
                    //final byte[] intermediateData = ((byte[]) intermediateDataStream[0]);
                    final byte[] intermediateData = (byte[]) reducerInput[j * 3 + 0];
                    fos.write(intermediateData);
                    fos.flush();
                    fos.close();
                }

            } else if (job.getNumReduceTasks() == 0) {
                for (int j = 0; j < mapperCount; ++j) {
                    //final Object[] intermediateDataStream = reducerInput[j];
                    //final byte[] intermediateData = ((byte[]) intermediateDataStream[0]);
                    final byte[] intermediateData = (byte[]) reducerInput[j * 3 + 0];
                    outputConvertTasks.add(new OutputConverter(intermediateData));
                }

                //intermArray = null;
                //System.gc();

                final ThreadPoolExecutor OCExecutorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
                job.setReduceOutput(OCExecutorService.invokeAll(outputConvertTasks));
                OCExecutorService.shutdown();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        //long noBytes = MemoryUtil.deepMemoryUsageOf(job);
        //System.out.println("******* Job Object Size ********: " + noBytes);
    }

    public static List<KVP> cloneList(final List<KVP> list) throws CloneNotSupportedException {
        final List<KVP> clone = new ArrayList<KVP>(list.size());
        for (KVP item : list) {
            clone.add(new KVP((Text) item.k, (IntWritable) item.v));
        }
        return clone;
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
                len = Math.min(len, buf.remaining());
                buf.get(bytes, off, len);
                return len;
            }
        };
    }

    // BS for now
//    private TextIntRecordReader initialiseReader(final File input) throws URISyntaxException, IOException {
//        final TextIntRecordReader r = new TextIntRecordReader();
//        final FileInputStream fis = new FileInputStream(input);
//        final FileChannel fcin = fis.getChannel();
//        final ByteBuffer buffer = ByteBuffer.allocate((int) fcin.size());
//        fcin.read(buffer);
//        buffer.rewind();
//        r.initialize(buffer);
//        return r;
//    }
//    private TextIntRecordReader initialiseReader(final TreeMultiMap<WritableComparable, WritableComparable> input) throws URISyntaxException, FileNotFoundException, IOException {
//        final TextIntRecordReader r = new TextIntRecordReader();
//        r.initialize(input);
//        return r;
//    }
//
//    private TextIntRecordReader initialiseReader(final byte[] input) throws URISyntaxException, FileNotFoundException, IOException {
//        final TextIntRecordReader r = new TextIntRecordReader();
//        r.initialize(input);
//        return r;
//    }
    private RecordReader initialiseReader(final byte[] data, final byte[] offsets, int offsetsSize) throws URISyntaxException, IOException {
        RecordReader r = null;

        if (prop.getProperty("application").equals("wordcount")) {
            r = new TextIntRecordReader();
        } else if (prop.getProperty("application").equals("pagerank")) {
            r = new IntPageRankNodeRecordReader();
        } else if (prop.getProperty("application").equals("mrlda")) {
            r = new PairOfIntsDoubleRecordReader();
        } else if (prop.getProperty("application").equals("workloadgenerator")) {
            r = new IntTextRecordReader();
        } else if (prop.getProperty("application").equals("kmeans")) {
            //r = new IntTextRecordReader();
            r = new IntPairDoublePointIntNodeRecordReader();
        } else if (prop.getProperty("application").equals("invertedindexing")) {
            r = new TextPairOfIntsRecordReader();
        }
        r.initialize(data, offsets, offsetsSize);
        return r;
    }
}
