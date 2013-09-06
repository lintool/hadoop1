/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.test;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import org.apache.hadoop.io.DynamicDirectByteBuffer;
import org.apache.hadoop.util.ByteUtil;
import org.apache.hadoop.util.DirectByteBufferCleaner;

/**
 *
 * @author ashwinkayyoor
 */
public class ByteBufferTests {

    public static void main(String[] args) throws CharacterCodingException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, SecurityException, NoSuchMethodException, NoSuchFieldException, UnsupportedEncodingException {

        // Allocate a new non-direct byte buffer with a 50 byte capacity


        // set this to a big value to avoid BufferOverflowException
        ByteBuffer buf = ByteBuffer.allocateDirect(30);
        //buf.("Java Code Geeks");

//        // Creates a view of this byte buffer as a char buffer
        CharBuffer cbuf = buf.asCharBuffer();
//
//        // Write a string to char buffer
        cbuf.put("Java Code Geeks");

        // Returns a charset object for the named charset.
        Charset charset = Charset.forName("UTF-8");

        // Constructs a new encoder for this charset.
        CharsetEncoder encoder = charset.newEncoder();
//
//        // Flips this buffer.  The limit is set to the current position and then
//        // the position is set to zero.  If the mark is defined then it is discarded
        cbuf.flip();
        ByteBuffer buf1 = encoder.encode(cbuf);
        //char s = buf.getChar();  // a string
        buf.put(buf1);
        byte b = 1;
        buf.put(b);
        buf.putInt(1);
        buf.putInt(1);
        buf.putInt(1);
        //buf.put(b);
        //ByteBuffer growBuf = ByteBuffer.allocateDirect(32);
        //buf.flip();
        //growBuf.put(buf);
        //DirectByteBufferCleaner.free(buf);
        //buf = null;
        //buf = growBuf;
        //buf.putInt(1);
        //growBuf.clear();

        byte[] byteArray = new byte[buf.position()];
        buf.clear();
        buf.get(byteArray);
        //buf.putInt(2);
        //buf.put(b)
        //buf = null;
        //growBuf=null;
        //DirectByteBufferCleaner.free(growBuf);
        //DirectByteBufferCleaner.free(buf);
        //System.gc();
        //buf.flip();
        //buf.get();   
        //buf.get();
        //buf.get();
        buf.clear();
        System.out.println(buf.hasArray() + " " + buf.position() + " " + byteArray.length+" "+(buf.limit()-buf.position())+" "+buf.remaining());
        
        System.out.println("Dynamic Byte Buffer Testing starts ....");
        DynamicDirectByteBuffer ddbb = new DynamicDirectByteBuffer(70);
        /*for(int i=0;i<1000000;++i){
            ddbb.putString("ashwin");
        }*/
        ddbb.putString("ashwin kumar");
        ddbb.putString("vijay kumar");
//        ddbb.putInt(1);
//        ddbb.putInt(2);
//        ddbb.putInt(3);
//        ddbb.putInt(4);
        byte[] array = null;
        //= new byte[ddbb.position()];
        ddbb.clear();
        array = ddbb.get(array);
        //System.out.println(ByteUtil.readInt(array, 0)+" "+ByteUtil.readInt(array, 4));
        System.out.println(ByteUtil.readString(array, 2, 13)+" "+ByteUtil.readString(array, 15, array.length-1));
        System.out.println(ddbb.position()+" "+array.length);
    }
}
