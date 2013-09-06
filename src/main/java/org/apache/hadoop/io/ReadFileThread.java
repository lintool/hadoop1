/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.io;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.mapreduce.lib.input.InputFormat;

/**
 *
 * @author ashwinkayyoor
 */
public class ReadFileThread implements Callable {

    String filename;
    File file;
    InputFormat inputFormat;

    public ReadFileThread(String filename, InputFormat inputFormat) {
        this.filename = filename;
        this.inputFormat = inputFormat;
    }

    public ReadFileThread(File file, InputFormat inputFormat) {
        this.file = file;
        this.inputFormat = inputFormat;
    }

//    public static byte[] load(String fileName) {
//        try {
//            FileInputStream fin = new FileInputStream(fileName);
//            return load(fin);
//        } catch (Exception e) {
//
//            return new byte[0];
//        }
//    }
//
//    public static byte[] load(File file) {
//        try {
//            FileInputStream fin = new FileInputStream(file);
//            return load(fin);
//        } catch (Exception e) {
//
//            return new byte[0];
//        }
//    }
//
//    public static byte[] load(FileInputStream fin) {
//        byte readBuf[] = new byte[512 * 1024];
//
//        try {
//            ByteArrayOutputStream bout = new ByteArrayOutputStream();
//
//            int readCnt = fin.read(readBuf);
//            while (0 < readCnt) {
//                bout.write(readBuf, 0, readCnt);
//                readCnt = fin.read(readBuf);
//                //Thread.yield();
//            }
//
//            fin.close();
//
//            return bout.toByteArray();
//        } catch (Exception e) {
//
//            return new byte[0];
//        }
//    }

    public ByteBuffer load(File file) {
        FileChannel roChannel=null;
        //ByteBuffer roBuf = inputFormat.getByteBuffer(file);
        try {
            roChannel = new RandomAccessFile(file, "r").getChannel();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ReadFileThread.class.getName()).log(Level.SEVERE, null, ex);
        }
        ByteBuffer roBuf = null;
        try {
            roBuf = roChannel.map(FileChannel.MapMode.READ_ONLY, 0, (int) roChannel.size());
        } catch (IOException ex) {
            Logger.getLogger(ReadFileThread.class.getName()).log(Level.SEVERE, null, ex);
        }
        return roBuf;
    }

    @Override
    public Object call() throws Exception {

        //Properties prop = new Properties();
        //prop.load(new FileInputStream("config.properties"));

        //byte[] barray = load(file);
        
        return load(file);
    }
}
