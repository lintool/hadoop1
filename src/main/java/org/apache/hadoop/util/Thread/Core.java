/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util.Thread;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class Core {

        private static final Map<Thread, Core> threadToCore = Collections.synchronizedMap(new IdentityHashMap<Thread, Core>());

        private final int chipId;
        private final int coreId;
        private final List<Processor> processors;
        private final String name;

        public Core(int chipId, int coreId, ArrayList<Processor> processors) {
                if (processors == null || processors.size() == 0) {
                        throw new AffinityException("A core must have at least one processor!");
                } else if (processors.size() > 2) {
                        throw new AffinityException("Can only handle a max of 2 processors per core: " + processors.size());
                }
                this.chipId = chipId;
                this.coreId = coreId;
                this.processors = Collections.unmodifiableList(processors);
                this.name = "Core-" + coreId + "." + chipId;
        }

        public int getCoreId() {
                return coreId;
        }

        public int getChipId() {
                return chipId;
        }

        public List<Processor> getProcessors() {
                return processors;
        }

        public int getNumberOfProcessors() {
                return processors.size();
        }

        static Core getBoundCoreForThread(Thread t) {
                return threadToCore.get(t);
        }

        synchronized void bind(Thread t1, Thread t2) {
                if (getNumberOfProcessors() != 2) {
                        throw new AffinityException("The chip does not support hyperthreading!");
                }
                if (!isFullyFree()) {
                        throw new AffinityException("This core is not fully free!");
                }
                processors.get(0).bind(t1);
                processors.get(1).bind(t2);
        }

        public synchronized boolean isFullyFree() {
                for (int i = 0; i < processors.size(); i++) {
                        Processor p = processors.get(i);
                        if (!p.isFree()) {
                                return false;
                        }
                }
                return true;
        }

        public synchronized boolean hasFreeProcessor() {
                return getFreeProcessor() != null;
        }

        public synchronized Processor getFreeProcessor() {
                for (int i = 0; i < processors.size(); i++) {
                        Processor p = processors.get(i);
                        if (p.isFree()) {
                                return p;
                        }
                }
                return null;
        }

        synchronized void bind(Thread thread) {
                if (!isFullyFree()) {
                        throw new AffinityException("Core is not fully free!");
                }
                if (threadToCore.containsKey(thread)) {
                        throw new AffinityException("Thread is already bound to a core!");
                }
                if (processors.size() == 1) {
                        processors.get(0).bind(thread);
                } else {
                        processors.get(0).bind(thread);
                        processors.get(1).setIdle(thread, true);
                }
                threadToCore.put(thread, this);
        }

        synchronized void unbind(Thread thread) {
                if (!threadToCore.containsKey(thread)) {
                        throw new AffinityException("Thread is not bound to any core!");
                }

                if (processors.size() == 1) {
                        processors.get(0).unbind(thread);
                } else {
                        processors.get(0).unbind(thread);
                        processors.get(1).setIdle(thread, false);
                }
                threadToCore.remove(thread);
        }

        @Override
        public int hashCode() {
                return chipId * 31 + coreId;
        }

        @Override
        public boolean equals(Object obj) {
                if (obj instanceof Core) {
                        Core c = (Core) obj;
                        return c.coreId == this.coreId && c.chipId == this.chipId;
                }
                return false;
        }

        @Override
        public String toString() {
                return name;
        }
}