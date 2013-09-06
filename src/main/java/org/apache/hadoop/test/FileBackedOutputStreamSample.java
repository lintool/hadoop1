/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.google.common.base.Stopwatch;
import com.google.common.io.FileBackedOutputStream;

/**
 *
 * @author ashwinkayyoor
 */
public class FileBackedOutputStreamSample {

    private static final int KB = 1024;
    private static final int MB = 1024 * KB;
    private static final int GB = 1024 * MB;
    private static final int THRESHOLD = 32 * KB;
    private static final int[] TEST_SIZE = new int[]{16 * KB, 32 * KB, 128 * KB, 1 * MB, 32 * MB, 1024 * MB};

    public static void main(String[] args) throws IOException {
        for (int writeSize : TEST_SIZE) {
            testFileBackedOutputStream(writeSize);
            testByteArrayOutputStream(writeSize);
            System.out.println();
        }
    }

    private static void testFileBackedOutputStream(int writeSize) throws IOException {
        test(new FileBackedOutputStream(THRESHOLD), writeSize);
    }

    private static void testByteArrayOutputStream(int writeSize) throws IOException {
        test(new ByteArrayOutputStream(), writeSize);
    }

    private static void test(OutputStream oStream, int writeSize) throws IOException {
        BufferedOutputStream buffer = new BufferedOutputStream(oStream);
        byte[] bytes = new byte[KB];

        Stopwatch watch = new Stopwatch().start();
        try {
            for (int i = 0; i < writeSize / KB; ++i) {
                buffer.write(bytes);
            }
            watch.stop();
        } finally {
            buffer.close();
        }

        System.out.printf(
                "I\'ve finished to write %,12d bytes in %,6d msec via %s%n",
                writeSize,
                watch.elapsedMillis(),
                oStream.getClass().getName());
    }
}
