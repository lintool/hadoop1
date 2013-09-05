package org.apache.hadoop.util;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

// Goal: offer a generic external-memory sorting program in Java.
// 
// It must be : 
//  - hackable (easy to adapt)
//  - scalable to large files
//  - sensibly efficient.
// This software is in the public domain.
public class ParallelExternalSort {

    List<DataInputStream> streams;

    public ParallelExternalSort(List<DataInputStream> streams) {
        this.streams = streams;
    }
    // we divide the file into small blocks. If the blocks
    // are too small, we shall create too many temporary files. 
    // If they are too big, we shall be using too much memory. 

    public static long estimateBestSizeOfBlocks(File filetobesorted) {
        long sizeoffile = filetobesorted.length();
        // we don't want to open up much more than 1024 temporary files, better run
        // out of memory first. (Even 1024 is stretching it.)
        final int MAXTEMPFILES = 1024;
        long blocksize = sizeoffile / MAXTEMPFILES;
        // on the other hand, we don't want to create many temporary files
        // for naught. If blocksize is smaller than half the free memory, grow it.
        long freemem = Runtime.getRuntime().freeMemory();
        if (blocksize < freemem / 2) {
            blocksize = freemem / 2;
        } else {
            if (blocksize >= freemem) {
                System.err.println("We expect to run out of memory. ");
            }
        }
        return blocksize;
    }

    public static long estimateBestSizeOfBlocks(int sizeoffile) {
        //long sizeoffile = filetobesorted.length();
        // we don't want to open up much more than 1024 temporary files, better run
        // out of memory first. (Even 1024 is stretching it.)
        final int MAXTEMPFILES = 1024;
        long blocksize = sizeoffile / MAXTEMPFILES;
        // on the other hand, we don't want to create many temporary files
        // for naught. If blocksize is smaller than half the free memory, grow it.
        long freemem = Runtime.getRuntime().freeMemory();
        if (blocksize < freemem / 2) {
            blocksize = freemem / 2;
        } else {
            if (blocksize >= freemem) {
                System.err.println("We expect to run out of memory. ");
            }
        }
        return blocksize;
    }

    // This will simply load the file by blocks of x rows, then
    // sort them in-memory, and write the result to a bunch of 
    // temporary files that have to be merged later.
    // 
    // @param file some flat  file
    // @return a list of temporary flat files
    public List<File> sortInBatch(Properties prop, Comparator<String> cmp) throws IOException, InterruptedException, ExecutionException, CloneNotSupportedException {

        List<File> files = new ArrayList<File>();
        File dir = new File(prop.getProperty("ramdisk_path"));
        List<String> tmplist = new ArrayList<String>();
        String line = "";
        int value;
        DataInputStream fbr;
        //BufferedReader fbr;
        long blocksize;

        ThreadPoolExecutor executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        final List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();

        int len = streams.size();
        for (int i = 0; i < len; ++i) {
            // fbr = new BufferedReader(new FileReader(file));
            fbr = streams.get(i);
            //BufferedReader br = new BufferedReader(new InputStreamReader(fbr));
            blocksize = estimateBestSizeOfBlocks(64000000);// in bytes
            //blocksize=100000;
            try {
                try {
                    while (line != null) {
                        long currentblocksize = 0;// in bytes
                        while ((currentblocksize < blocksize) && ((line = fbr.readUTF()) != null) && (value=fbr.readInt())>-9999) { // as long as you have 2MB
                            //if (line.trim().length() > 0 && line.length() < 30) {
                            //line = line.concat(" ").concat(String.valueOf(fbr.readInt()));
                            tmplist.add(line);
                           // System.out.println(line);

                            currentblocksize += line.length(); // 2 + 40; // java uses 16 bits per character + 40 bytes of overhead (estimated)
                            //}
                        }
                        tasks.add(new SortAndSave(ParallelExternalSort.cloneList(tmplist), cmp, dir));
                        //files.add(sortAndSave(tmplist, cmp, dir));
                        tmplist.clear();
                    }
                } catch (EOFException oef) {
                    if (tmplist.size() > 0) {
                        tasks.add(new SortAndSave(ParallelExternalSort.cloneList(tmplist), cmp, dir));
                        tmplist.clear();
                    }
                }
            } finally {
                fbr.close();
            }

        }

        System.out.println("Done loading sorted tasks to executor pool");
        List<Future<Object>> invokeAll = executorService.invokeAll(tasks);
        executorService.shutdown();

        File file;
        for (Future<Object> fut : invokeAll) {
            file = (File) fut.get();
            files.add(file);
        }

        return files;
    }

    public static List<String> cloneList(List<String> list) throws CloneNotSupportedException {
        List<String> clone = new ArrayList<String>(list.size());
        for (String item : list) {
            clone.add(new String(item));
        }
        return clone;
    }

    public class SortAndSave implements Callable {

        List<String> tmplist;
        Comparator<String> cmp;
        File dir;

        public SortAndSave(List<String> tmplist, Comparator<String> cmp, File dir) {
            this.tmplist = tmplist;
            this.cmp = cmp;
            this.dir = dir;
        }

        @Override
        public File call() throws Exception {
            Collections.sort(tmplist, cmp);  // 
            File newtmpfile = File.createTempFile("sortInBatch", "flatfile", dir);
            newtmpfile.deleteOnExit();
            BufferedWriter fbw = new BufferedWriter(new FileWriter(newtmpfile));
            try {
                for (String r : tmplist) {
                    fbw.write(r);
                    fbw.newLine();
                }
            } finally {
                fbw.close();
            }
            return newtmpfile;
        }
    }

    // This merges a bunch of temporary flat files 
    // @param files
    // @param output file
    // @return The number of lines sorted. (P. Beaudoin)
    public static int mergeSortedFiles(List<File> files, File outputfile, final Comparator<String> cmp) throws IOException {
        PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(11,
                new Comparator<BinaryFileBuffer>() {

                    @Override
                    public int compare(BinaryFileBuffer i, BinaryFileBuffer j) {
                        return cmp.compare(i.peek(), j.peek());
                    }
                });
        for (File f : files) {
            BinaryFileBuffer bfb = new BinaryFileBuffer(f);
            if (!bfb.empty()) {
                pq.add(bfb);
            }
        }
        BufferedWriter fbw = new BufferedWriter(new FileWriter(outputfile));
        int rowcounter = 0;
        try {
            while (pq.size() > 0) {
                BinaryFileBuffer bfb = pq.poll();
                String r = bfb.pop();
                fbw.write(r);
                fbw.newLine();
                ++rowcounter;
                if (bfb.empty()) {
                    bfb.fbr.close();
                    bfb.originalfile.delete();// we don't need you anymore
                } else {
                    pq.add(bfb); // add it back
                }
            }
        } finally {
            fbw.close();
            for (BinaryFileBuffer bfb : pq) {
                bfb.close();
            }
        }
        return rowcounter;
    }

    /*
     * public static void main(String[] args) throws IOException,
     * InterruptedException, ExecutionException, CloneNotSupportedException { if
     * (args.length < 2) { System.out.println("please provide input and output
     * file names"); return; } Properties prop = new Properties(); prop.load(new
     * FileInputStream("config.properties"));
     *
     * String inputfile = args[0]; String outputfile = args[1];
     * Comparator<String> comparator = new Comparator<String>() {
     *
     * @Override public int compare(String r1, String r2) { return
     * r1.compareTo(r2); } }; List<File> l = sortInBatch(prop, comparator);
     * mergeSortedFiles(l, new File(outputfile), comparator); }
     */
}

class ParallelBinaryFileBuffer {

    public static int BUFFERSIZE = 2048;
    public BufferedReader fbr;
    public File originalfile;
    private String cache;
    private boolean empty;

    public ParallelBinaryFileBuffer(File f) throws IOException {
        originalfile = f;
        fbr = new BufferedReader(new FileReader(f), BUFFERSIZE);
        reload();
    }

    public boolean empty() {
        return empty;
    }

    private void reload() throws IOException {
        try {
            if ((this.cache = fbr.readLine()) == null) {
                empty = true;
                cache = null;
            } else {
                empty = false;
            }
        } catch (EOFException oef) {
            empty = true;
            cache = null;
        }
    }

    public void close() throws IOException {
        fbr.close();
    }

    public String peek() {
        if (empty()) {
            return null;
        }
        return cache.toString();
    }

    public String pop() throws IOException {
        String answer = peek();
        reload();
        return answer;
    }
}
