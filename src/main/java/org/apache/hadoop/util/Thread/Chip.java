/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util.Thread;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Chip {

        private final int chipId;
        private final List<Core> cores;
        private final String name;

        public Chip(int chipId, ArrayList<Core> cores) {
                this.chipId = chipId;
                this.cores = Collections.unmodifiableList(cores);
                this.name = "Chip-" + chipId;
        }

        public int getId() {
                return chipId;
        }

        public Core getCore(int coreId) {
                for (int i = 0; i < cores.size(); i++) {
                        Core core = cores.get(i);
                        if (core.getCoreId() == coreId) {
                                return core;
                        }
                }
                return null;
        }

        public List<Core> getCores() {
                return cores;
        }

        public synchronized boolean hasFullyFreeCore() {
                return getFullyFreeCore() != null;
        }

        public synchronized Core getFullyFreeCore() {
                for (int i = 0; i < cores.size(); i++) {
                        Core core = cores.get(i);
                        if (core.isFullyFree()) {
                                return core;
                        }
                }
                return null;
        }

        public synchronized boolean hasFreeCore() {
                return getFreeCore() != null;
        }

        public synchronized Core getFreeCore() {
                for (int i = 0; i < cores.size(); i++) {
                        Core core = cores.get(i);
                        if (core.hasFreeProcessor()) {
                                return core;
                        }
                }
                return null;
        }

        public synchronized int getNumberOfFreeCores() {
                int count = 0;
                for (int i = 0; i < cores.size(); i++) {
                        Core core = cores.get(i);
                        if (core.hasFreeProcessor()) {
                                count++;
                        }
                }
                return count;
        }

        @Override
        public int hashCode() {
                return chipId;
        }

        @Override
        public boolean equals(Object obj) {
                if (obj instanceof Chip) {
                        Chip c = (Chip) obj;
                        return c.chipId == this.chipId;
                }
                return false;
        }

        @Override
        public String toString() {
                return name;
        }

        public synchronized void printSituation() {
                StringBuilder sb = new StringBuilder(1024);
                sb.append(name).append(":\n");
                for (int i = 0; i < cores.size(); i++) {
                        Core core = cores.get(i);
                        sb.append("    ").append(core).append(":\n");
                        List<Processor> processors = core.getProcessors();
                        for (int j = 0; j < processors.size(); j++) {
                                Processor p = processors.get(j);
                                sb.append("        ").append(p).append(": ");
                                sb.append(p.getStatusString()).append("\n");
                        }
                }
                System.out.println(sb.toString());
        }
}
