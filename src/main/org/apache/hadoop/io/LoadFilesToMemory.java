/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.mapreduce.lib.input.InputFormat;

/**
 *
 * @author ashwinkayyoor
 */
public class LoadFilesToMemory {

    InputFormat inputFormat;
    public static final char lineTerminator = '\n';
    private static int noOfSplits = 0;

    public LoadFilesToMemory(InputFormat inputFormat) {
        this.inputFormat = inputFormat;
    }

    public final List<Future<Object>> getByteBuffers(final String inputDirectoryPath) throws InterruptedException, IOException {
        final Properties prop = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream("config.properties");
        } catch (FileNotFoundException ex) {
            Logger.getLogger(LoadFilesToMemory.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            prop.load(fis);
        } catch (IOException ex) {
            Logger.getLogger(LoadFilesToMemory.class.getName()).log(Level.SEVERE, null, ex);
        }

        final ThreadPoolExecutor executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(Integer.parseInt(prop.getProperty("poolThreads")));
        final List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();
        prop.clear();
        fis.close();

        //File dir = new File(prop.getProperty("inputSplitsPath"));
        final File dir = new File(inputDirectoryPath);
        for (File child : dir.listFiles()) {
            if (".".equals(child.getName()) || "..".equals(child.getName())) {
                continue;  // Ignore the self and parent aliases.
            }
            if (child.getName().toCharArray()[0] != '.') {
                tasks.add(new ReadFileThread(child, inputFormat));
                noOfSplits++;
            }
        }
        //prop=null;
        final List<Future<Object>> invokeAll = executorService.invokeAll(tasks);
        executorService.shutdown();

        return invokeAll;
    }

    public final static int getNoOfSplits() {
        return noOfSplits;
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        // TODO code application logic here
        final LoadFilesToMemory loadToMemory = new LoadFilesToMemory(null);
        final List<Future<Object>> byteBufferList = loadToMemory.getByteBuffers("data/wikipedia-graph");

        ByteBuffer byteBuffer;

        final StringBuilder sb = new StringBuilder();
        for (Future< Object> fut : byteBufferList) {
            byteBuffer = (ByteBuffer) fut.get();

            while (byteBuffer.hasRemaining()) {
                final char value = (char) (byteBuffer.get() & 0x000000FF);

                if (lineTerminator == value || !byteBuffer.hasRemaining()) {
                    if (!byteBuffer.hasRemaining()) {
                        sb.append(value);
                        //Thread.yield();
                    }

                    // rowNumber++;
                    //rowText.set(sb.toString());
                    //System.out.println(sb.toString());
                    //Thread.yield();
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    //return true;
                } else {
                    sb.append(value);
                    //Thread.yield();
                }
                //Thread.yield();
            }
        }
    }
}
