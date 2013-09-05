/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util.Thread;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

public class Processor {

        private static final Map<Thread, Processor> threadToProcessor = Collections.synchronizedMap(new IdentityHashMap<Thread, Processor>());

        private final int procId;
        private final int chipId;
        private final int coreId;
        private final boolean isExcluded;
        private boolean isBound = false;
        private boolean leaveIdle = false;
        private Thread thread = null;
        private final String name;

        public Processor(final int procId, final int chipId, final int coreId, final boolean isExcluded) {
                this.procId = procId;
                this.chipId = chipId;
                this.coreId = coreId;
                this.isExcluded = isExcluded;
                this.name = "Processor-" + procId;
        }

        public synchronized String getStatusString() {
                if (isExcluded) {
                        return "excluded";
                } else if (leaveIdle) {
                        return "left idle by " + thread.getName();
                } else if (isBound) {
                        return "bound to " + thread.getName();
                } else {
                        return "free";
                }
        }

        public int getId() {
                return procId;
        }

        public int getChipId() {
                return chipId;
        }

        public int getCoreId() {
                return coreId;
        }

        public synchronized boolean isFree() {
                return !isBound && !isExcluded && !leaveIdle;
        }

        public synchronized boolean isBound() {
                return isBound;
        }

        public synchronized Thread getThread() {
                return thread;
        }

        synchronized void bind(Thread thread) {
                if (isBound) {
                        throw new AffinityException("Processor is already bound to a thread: " + this.thread);
                }
                if (threadToProcessor.containsKey(thread)) {
                        throw new AffinityException("Thread is already bound to a processor!");
                }
                this.isBound = true;
                this.thread = thread;
                threadToProcessor.put(thread, this);
        }

        synchronized void setIdle(Thread thread, boolean flag) {
                if (!flag) {
                        if (this.thread != thread) {
                                throw new AffinityException("Processor was not made idle by this thread: " + this.thread);
                        }
                        this.leaveIdle = false;
                        this.thread = null;
                } else {
                        if (this.leaveIdle) {
                                throw new AffinityException("Processor is already idle: " + this.thread);
                        }
                        this.leaveIdle = true;
                        this.thread = thread;
                }
        }

        synchronized void unbind(Thread thread) {
                if (this.thread != thread) {
                        throw new AffinityException("Processor is bound to another thread: " + this.thread);
                }
                if (!threadToProcessor.containsKey(thread)) {
                        throw new AffinityException("Thread is not bound to any processor!");
                }
                this.isBound = false;
                this.thread = null;
                threadToProcessor.remove(thread);
        }

        public boolean sameChip(Processor other) {
                return other.chipId == this.chipId;
        }

        public boolean sameCore(Processor other) {
                // core ids will be the SAME per chip... so be careful...
                return sameChip(other) && other.coreId == this.coreId;
        }

        public boolean sameChipDifferentCore(Processor other) {
                return sameChip(other) && other.coreId != this.coreId;
        }

        @Override
        public int hashCode() {
                return procId;
        }

        static Processor getBoundProcessorForThread(Thread t) {
                return threadToProcessor.get(t);
        }

        @Override
        public boolean equals(Object o) {
                if (o instanceof Processor) {
                        Processor lp = (Processor) o;
                        return lp.procId == this.procId;
                }
                return false;
        }

        @Override
        public String toString() {
                return name;
        }

        public int mask() {
                return 1 << procId;
        }
}
