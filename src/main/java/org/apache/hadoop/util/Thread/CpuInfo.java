/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util.Thread;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

public class CpuInfo {

        private static final String DEFAULT_CPUINFO = "/proc/cpuinfo";

        private final boolean hasHyperthreading;
        private final int nChips;
        private final int nProcessors;
        private final int nCores;
        private final List<Chip> chips;

        private static CpuInfo INSTANCE;

        public static void init(String filename) {
                if (INSTANCE != null) {
                        throw new AffinityException("CpuInfo is already initialized!");
                }
                INSTANCE = new CpuInfo(filename);
        }

        public static void init() {
                init(DEFAULT_CPUINFO);
        }

        public static CpuInfo instance() {
                if (INSTANCE == null) {
                        throw new AffinityException("CpuInfo is not initialized!");
                }
                return INSTANCE;
        }

        public static void printInfo() {
                System.out.println(instance().toString());
        }

        private CpuInfo(String filename) {
                // suck everything to memory...
                List<String> lines = new ArrayList<String>(256);
                File file = new File(filename);
                if (!file.exists()) {
                        throw new AffinityException("Cannot parse file: " + filename);
                }
                try {
                        FileInputStream fis = new FileInputStream(file);
                        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                        String line;
                        while ((line = br.readLine()) != null) {
                                lines.add(line.trim());
                        }
                        br.close();
                        fis.close();
                } catch (Exception e) {
                        throw new AffinityException("Error parsing file: " + filename, e);
                }

                // count number of physical processors...
                Set<Integer> chips = new HashSet<Integer>();
                for (String line : lines) {
                        if (line.toLowerCase().startsWith("physical id")) {
                                chips.add(parseNumber(line));
                        }
                }
                this.nChips = chips.size();

                // number of logical processors
                this.nProcessors = countLines(lines, "processor");
                int check = getUniqueNumber(lines, "siblings");
                if (nProcessors != check) {
                        throw new AffinityException("Number of logical processors does not match siblings!");
                }

                this.nCores = getUniqueNumber(lines, "cpu cores");

                this.hasHyperthreading = this.nCores != this.nProcessors;

                String all = join(lines).trim();

                String[] procBlock = all.split("\n\n");

                Set<Processor> allProcessors = new HashSet<Processor>();
                Set<String> ensureUnique = new HashSet<String>();

                for (String s : procBlock) {
                        String[] t = s.split("\n");
                        int chipId = getNumber(t, "physical id");
                        int coreId = getNumber(t, "core id");
                        int procId = getNumber(t, "processor");
                        String unique = chipId + "-" + coreId + "-" + procId;
                        if (ensureUnique.contains(unique)) {
                                throw new AffinityException("Found duplicate processor specification: " + unique);
                        } else {
                                ensureUnique.add(unique);
                        }
                        allProcessors.add(new Processor(procId, chipId, coreId, false));
                }

                Set<String> allCores = new HashSet<String>();

                // create all cores from processors...
                for (Processor p : allProcessors) {
                        allCores.add(p.getChipId() + "-" + p.getCoreId());
                }

                Set<Core> theCores = new HashSet<Core>();

                // now get all processors for each core so you can create them:
                for (String s : allCores) {
                        String[] t = s.split("\\-");
                        int chipId = Integer.parseInt(t[0]);
                        int coreId = Integer.parseInt(t[1]);
                        ArrayList<Processor> procs = new ArrayList<Processor>(2);
                        for (Processor p : allProcessors) {
                                if (p.getChipId() == chipId && p.getCoreId() == coreId) {
                                        procs.add(p);
                                }
                        }
                        theCores.add(new Core(chipId, coreId, procs));
                }

                // create all chips from processors...
                Set<Integer> allChips = new HashSet<Integer>();
                for (Processor p : allProcessors) {
                        allChips.add(p.getChipId());
                }

                List<Chip> theChips = new ArrayList<Chip>();

                // now get all cores for each chip so you can create them:
                for (int chipId : allChips) {
                        ArrayList<Core> cores = new ArrayList<Core>();
                        for (Core c : theCores) {
                                if (c.getChipId() == chipId) {
                                        cores.add(c);
                                }
                        }
                        theChips.add(new Chip(chipId, cores));
                }

                this.chips = Collections.unmodifiableList(theChips);
        }

        private static String join(List<String> lines) {
                StringBuilder sb = new StringBuilder(1024);
                for (String line : lines) {
                        sb.append(line).append('\n');
                }
                return sb.toString();
        }

        public static List<Chip> getChips() {
                return instance().chips;
        }

        public static Chip getChip() {
                if (CpuInfo.getChips().size() != 1) {
                        throw new AffinityException("Your machine has more than one processor!");
                }
                return getChips().get(0);
        }

        @Override
        public String toString() {
                StringBuilder sb = new StringBuilder(128);
                sb.append("CpuInfo: [nChips=").append(nChips).append(", ");
                sb.append("nCoresPerChip=").append(nCores / nChips).append(", ");
                sb.append("hyperthreading=").append(hasHyperthreading).append(", ");
                sb.append("nProcessors=").append(nProcessors).append("]");
                return sb.toString();
        }

        private static int parseNumber(String line) {
                String[] temp = line.split("\\:");
                if (temp.length != 2) {
                        throw new AffinityException("Cannot parse id from line: " + line);
                }
                return Integer.parseInt(temp[1].trim());
        }

        private static int countLines(List<String> lines, String s) {
                int x = 0;
                for (String line : lines) {
                        if (line.toLowerCase().startsWith(s)) {
                                x++;
                        }
                }
                return x;
        }

        private static int getNumber(String[] lines, String s) {
                for (String line : lines) {
                        if (line.toLowerCase().startsWith(s)) {
                                return parseNumber(line);
                        }
                }
                throw new AffinityException("Cannot find number: " + s);
        }

        private static int getUniqueNumber(List<String> lines, String s) {
                int x = -1;
                for (String line : lines) {
                        if (line.toLowerCase().startsWith(s)) {
                                int n = parseNumber(line);
                                if (x == -1) {
                                        x = n;
                                } else if (x != n) {
                                        throw new AffinityException("Field is not unique: " + s);
                                }
                        }
                }
                return x;
        }

        public static void main(String[] args) throws Exception {
                if (args.length == 0) {
                        CpuInfo.init();
                } else {
                        CpuInfo.init(args[0]);
                }
                CpuInfo.printInfo();
                Affinity.printSituation();

                Thread t1 = new Thread("Thread1") {

                        @Override
                        public void run() {
                                Affinity.bind();
                                try {
                                        long count = 0;
                                        while (true) {
                                                while (count < Integer.MAX_VALUE) {
                                                        count++;
                                                }
                                                try {
                                                        Thread.sleep(500);
                                                } catch (Exception e) {
                                                        return;
                                                }
                                                count = 0;
                                        }
                                } finally {
                                        Affinity.unbind();
                                }
                        }
                };

                Thread t2 = new Thread("Thread2") {

                        @Override
                        public void run() {
                                Affinity.bind();
                                try {
                                        long count = 0;
                                        while (true) {
                                                while (count < Integer.MAX_VALUE) {
                                                        count++;
                                                }
                                                try {
                                                        Thread.sleep(500);
                                                } catch (Exception e) {
                                                        return;
                                                }
                                                count = 0;
                                        }
                                } finally {
                                        Affinity.unbind();
                                }
                        }
                };

                //              Affinity.bindToAnyFreeProcessor(t1);
                //              Affinity.bindToAnyFreeProcessor(t2);

                Affinity.bindToProcessor(1, t1);
                Affinity.bindToProcessor(4, t2);

                Affinity.printSituation();

                t1.start();
                t2.start();

                t1.join();
                t2.join();

        }
}