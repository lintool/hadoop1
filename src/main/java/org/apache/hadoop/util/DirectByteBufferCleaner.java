/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import sun.misc.Cleaner;

/**
 *
 * @author ashwinkayyoor
 */
public final class DirectByteBufferCleaner {

    private DirectByteBufferCleaner() {
    }

    public static void clean(final ByteBuffer byteBuffer) {
        if (!byteBuffer.isDirect()) {
            return;
        }
        try {
            Object cleaner = invoke(byteBuffer, "cleaner");
            invoke(cleaner, "clean");
        } catch (Exception e) { /*
             * ignore
             */ }
    }

    private static Object invoke(final Object target, String methodName) throws Exception {
        final Method method = target.getClass().getMethod(methodName);
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {

            @Override
            public Object run() {
                try {
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public static void destroyDirectByteBuffer(final ByteBuffer buffer)
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, SecurityException,
            NoSuchMethodException {

        if (buffer.isDirect()) {
            Method cleanerMethod = buffer.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(buffer);
            Method cleanMethod = cleaner.getClass().getMethod("clean");
            cleanMethod.setAccessible(true);
            cleanMethod.invoke(cleaner);
        } else {
            System.out.println("toBeDestroyed isn't direct!");
        }
    }

    public static void free(final ByteBuffer buffer) throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Field cleanerField = buffer.getClass().getDeclaredField("cleaner");
        cleanerField.setAccessible(true);
        Cleaner cleaner = (Cleaner) cleanerField.get(buffer);
        cleaner.clean();
    }
}