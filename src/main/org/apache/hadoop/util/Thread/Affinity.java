/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util.Thread;

import java.util.List;

public class Affinity {

        private static Chip getSingleChip() {
                final List<Chip> chips = CpuInfo.getChips();
                if (chips.size() != 1) {
                        throw new AffinityException("Your computer has more than one chip so you must specify when binding!");
                }
                return chips.get(0);
        }

        public synchronized static Processor getProcessor(int id) {
                final List<Chip> chips = CpuInfo.getChips();
                for (int i = 0; i < chips.size(); i++) {
                        final List<Core> cores = chips.get(i).getCores();
                        for (int j = 0; j < cores.size(); j++) {
                                final List<Processor> procs = cores.get(j).getProcessors();
                                for (int k = 0; k < procs.size(); k++) {
                                        if (procs.get(k).getId() == id) {
                                                return procs.get(k);
                                        }
                                }
                        }
                }
                return null;
        }

        public synchronized static void bindToProcessor(Processor p, Thread t) {
                p.bind(t);
        }

        public synchronized static void bindToProcessor(int procId, Thread t) {
                final Processor p = getProcessor(procId);
                if (p == null) {
                        throw new AffinityException("Cannot find processor: " + procId);
                }
                bindToProcessor(p, t);
        }

        public synchronized static Processor bindToAnyFreeProcessor(Thread t) {
                return bindToAnyFreeProcessor(getSingleChip(), t);
        }

        public synchronized static Processor bindToAnyFreeProcessor(Chip chip, Thread t) {
                final Core core = chip.getFreeCore();
                if (core == null) {
                        throw new AffinityException("Cannot find a free core from chip: " + chip);
                }
                final Processor processor = core.getFreeProcessor();
                processor.bind(t);
                return processor;
        }

        public synchronized static Core bindToFullyFreeCore(Thread t) {
                return bindToFullyFreeCore(getSingleChip(), t);
        }

        public synchronized static Core bindToFullyFreeCore(Chip chip, Thread t) {
                final Core core = chip.getFullyFreeCore();
                if (core == null) {
                        throw new AffinityException("Cannot find a fully free core from chip: " + chip);
                }
                core.bind(t);
                return core;
        }

        public synchronized static void bindToTheSameCore(Thread t1, Thread t2) {
                bindToTheSameCore(getSingleChip(), t1, t2);
        }

        public synchronized static void bindToTheSameCore(Chip chip, Thread t1, Thread t2) {
                final Core core = chip.getFullyFreeCore();
                if (core == null) {
                        throw new AffinityException("Cannot find a fully free core from chip: " + chip);
                }
                core.bind(t1, t2);
        }

        public synchronized static void bindToDifferentCores(Thread t1, Thread t2) {
                bindToDifferentCores(getSingleChip(), t1, t2);
        }

        public synchronized static void bindToDifferentCores(Chip chip, Thread t1, Thread t2) {
                if (chip.getNumberOfFreeCores() >= 2) {
                        final Core core1 = chip.getFreeCore();
                        core1.bind(t1);
                        final Core core2 = chip.getFreeCore();
                        core2.bind(t2);
                } else {
                        throw new AffinityException("There are not two free cores in this chip: " + chip);
                }
        }

        public synchronized static void bindToDifferentChips(Thread t1, Thread t2) {
                final List<Chip> chips = CpuInfo.getChips();
                if (chips.size() == 1) {
                        throw new AffinityException("Your computer only have one chip!");
                }
                if (chips.size() > 2) {
                        throw new AffinityException("Your computer has more than two chips so you must specify the chips to bind!");
                }
                bindToDifferentChips(chips.get(0), t1, chips.get(1), t2);
        }

        public synchronized static void bindToDifferentChips(Chip chip1, Thread t1, Chip chip2, Thread t2) {
                if (!chip1.hasFreeCore()) {
                        throw new AffinityException("This chip does not have a free core: " + chip1);
                }
                if (!chip2.hasFreeCore()) {
                        throw new AffinityException("This chip does not have a free core: " + chip2);
                }
                chip1.getFreeCore().getFreeProcessor().bind(t1);
                chip2.getFreeCore().getFreeProcessor().bind(t2);
        }

        public synchronized static void bind() {
                final Thread t = Thread.currentThread();
                // first check core bind...
                final Core core = Core.getBoundCoreForThread(t);
                if (core != null) {
                        // check which processor you have to bind...
                        for (int i = 0; i < core.getProcessors().size(); i++) {
                                final Processor p = core.getProcessors().get(i);
                                if (p.isBound()) {
                                        if (p.getThread() != t) {
                                                throw new AffinityException("Cannot bind to this processor: " + p.getThread().getName());
                                        }
                                        try {
                                                ThreadAffinity.setCurrentThreadAffinityMask(p.mask());
                                                return;
                                        } catch (Exception e) {
                                                throw new AffinityException("Cannot bind with native code!", e);
                                        }
                                }
                        }

                        throw new AffinityException("Error binding to core: " + core);
                }
                // now check for processor
                final Processor processor = Processor.getBoundProcessorForThread(t);
                if (processor != null) {
                        if (processor.isBound()) {
                                if (processor.getThread() != t) {
                                        throw new AffinityException("Cannot bind to this processor: " + processor.getThread().getName());
                                }
                                try {
                                        ThreadAffinity.setCurrentThreadAffinityMask(processor.mask());
                                        return;
                                } catch (Exception e) {
                                        throw new AffinityException("Cannot bind with native code!", e);
                                }
                        }
                        throw new AffinityException("Error binding to processor: " + processor);
                }
                throw new AffinityException("Cannot find processor to bind thread: " + t);
        }

        public synchronized static void unbind() {
                final Thread t = Thread.currentThread();
                // first check core bind...
                final Core core = Core.getBoundCoreForThread(t);
                if (core != null) {
                        core.unbind(t);
                        return;
                }
                // now check for processor...
                final Processor processor = Processor.getBoundProcessorForThread(t);
                if (processor != null) {
                        processor.unbind(t);
                        return;
                }
                throw new AffinityException("Cannot find processor to unbind thread: " + t);
        }

        public synchronized static void printSituation() {
                final List<Chip> chips = CpuInfo.getChips();
                for (int i = 0; i < chips.size(); i++) {
                        final Chip chip = chips.get(i);
                        chip.printSituation();
                }
        }
}