/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

/**
 *
 * @author ashwinkayyoor
 */
public class OutputConverter implements Callable {

    byte[] bytesArray;

    public OutputConverter(byte[] byteArray) {
        this.bytesArray = byteArray;
    }

    @Override
    public Object call() throws Exception {
        return ByteBuffer.wrap(bytesArray);
    }
}
