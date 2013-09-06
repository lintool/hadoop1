/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util.Thread;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.PointerType;
import com.sun.jna.ptr.LongByReference;

/**
 * For attaching threads to cores
 * 
 * SOURCE: http://stackoverflow.com/questions/2238272/java-thread-affinity
 * 
 * @author cheremin
 * @since 25.10.11, 14:18
 */
class ThreadAffinity {

        public static void setCurrentThreadAffinityMask(final long mask) throws Exception {
                final CLibrary lib = CLibrary.INSTANCE;
                final int cpuMaskSize = Long.SIZE / 8;
                try {
                        final int ret = lib.sched_setaffinity(0, cpuMaskSize, new LongByReference(mask));
                        if (ret < 0) {
                                throw new Exception("sched_setaffinity( 0, (" + cpuMaskSize + ") , &(" + mask + ") ) return " + ret);
                        }
                } catch (final Throwable e) {
                        throw new Exception(e);
                }
        }

        // NOT WORKING :( :( :(

        //      public static void setThreadAffinityMask(final long threadID, final long mask) throws Exception {
        //              final CLibrary lib = CLibrary.INSTANCE;
        //              final int cpuMaskSize = Long.SIZE / 8;
        //              try {
        //                      final int ret = lib.sched_setaffinity((int) threadID, cpuMaskSize, new LongByReference(mask));
        //                      if (ret < 0) {
        //                              throw new Exception("sched_setaffinity( " + threadID + ", (" + cpuMaskSize + ") , &(" + mask + ") ) return " + ret);
        //                      }
        //              } catch (final Throwable e) {
        //                      throw new Exception(e);
        //              }
        //      }

        public static int getCurrentProcessor() {
                return CLibrary.INSTANCE.sched_getcpu();
        }

        public static void nice(final int increment) throws Exception {
                final CLibrary lib = CLibrary.INSTANCE;
                try {
                        final int ret = lib.nice(increment);
                        if (ret < 0) {
                                throw new Exception("nice( " + increment + " ) return " + ret);
                        }
                } catch (final Throwable e) {
                        throw new Exception(e);
                }
        }

        private interface CLibrary extends Library {
                public static final CLibrary INSTANCE = (CLibrary) Native.loadLibrary("c", CLibrary.class);

                public int nice(final int increment) throws LastErrorException;

                public int sched_setaffinity(final int pid, final int cpusetsize, final PointerType cpuset) throws LastErrorException;

                public int sched_getcpu() throws LastErrorException;
        }
}
 