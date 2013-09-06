package test.hone.pagerank;

/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import com.google.common.base.Preconditions;
import edu.umd.cloud9.util.map.HMapIF;
import edu.umd.cloud9.util.map.MapIF;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Combiner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NonSplitableSequenceMemoryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceMemoryOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.ArrayListOfIntsWritable;

/**
 * <p> Main driver program for running the basic (non-Schimmy) implementation of
 * PageRank. Command-line arguments are as follows: </p>
 *
 * <ul> <li>[basePath]: the base path</li> <li>[numNodes]: number of nodes in
 * the graph</li> <li>[start]: starting iteration</li> <li>[end]: ending
 * iteration</li> <li>[useCombiner?]: 1 for using combiner, 0 for not</li>
 * <li>[useInMapCombiner?]: 1 for using in-mapper combining, 0 for not</li>
 * <li>[useRange?]: 1 for range partitioning, 0 for not</li> </ul>
 *
 * <p> The starting and ending iterations will correspond to paths
 * <code>/base/path/iterXXXX</code> and
 * <code>/base/path/iterYYYY</code>. As a example, if you specify 0 and 10 as
 * the starting and ending iterations, the driver program will start with the
 * graph structure stored at
 * <code>/base/path/iter0000</code>; final results will be stored at
 * <code>/base/path/iter0010</code>. </p>
 *
 * @see RunPageRankSchimmy
 * @author Jimmy Lin
 * @author Michael Schatz
 *
 */
public class RunPageRankBasic {
    //private static final Logger LOG = Logger.getLogger(RunPageRankBasic.class);

    private static int taskId;
    private static HashMap<String, ArrayList<Float>> massMap;

    private static enum PageRank {

        nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
    };

    // Mapper, no in-mapper combining.
    public static class MapClass extends Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

        // The neighbor to which we're sending messages.
        private final IntWritable neighbor = new IntWritable();
        // Contents of the messages: partial PageRank mass.
        private final PageRankNode intermediateMass = new PageRankNode();
        // For passing along node structure.
        private final PageRankNode intermediateStructure = new PageRankNode();

        @Override
        public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
                InterruptedException {

            // Pass along node structure.
            intermediateStructure.setNodeId(node.getNodeId());
            intermediateStructure.setType(PageRankNode.Type.Structure);
            intermediateStructure.setAdjacencyList(node.getAdjacenyList());

            context.write(nid, intermediateStructure);

            int massMessages = 0;

            // Distribute PageRank mass to neighbors (along outgoing edges).
            if (node.getAdjacenyList().size() > 0) {
                // Each neighbor gets an equal share of PageRank mass.
                ArrayListOfIntsWritable list = node.getAdjacenyList();
                float mass = node.getPageRank() - (float) StrictMath.log(list.size());

                //context.getCounter(PageRank.edges).increment(list.size());

                // Iterate over neighbors.
                for (int i = 0; i < list.size(); i++) {
                    neighbor.set(list.get(i));
                    intermediateMass.setNodeId(list.get(i));
                    intermediateMass.setType(PageRankNode.Type.Mass);
                    intermediateMass.setPageRank(mass);

                    // Emit messages with PageRank mass to neighbors.
                    context.write(neighbor, intermediateMass);
                    massMessages++;
                }
            }

            // Bookkeeping.
//      context.getCounter(PageRank.nodes).increment(1);
//      context.getCounter(PageRank.massMessages).increment(massMessages);
        }
    }

    // Mapper with in-mapper combiner optimization.
    public static class MapWithInMapperCombiningClass extends Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

        // For buffering PageRank mass contributes keyed by destination node.
        private static final HMapIF map = new HMapIF();
        // For passing along node structure.
        private static final PageRankNode intermediateStructure = new PageRankNode();

        @Override
        public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
                InterruptedException {

            // Pass along node structure.
            intermediateStructure.setNodeId(node.getNodeId());
            intermediateStructure.setType(PageRankNode.Type.Structure);
            intermediateStructure.setAdjacencyList(node.getAdjacenyList());

            context.write(nid, intermediateStructure);

            int massMessages = 0;
            int massMessagesSaved = 0;

            // Distribute PageRank mass to neighbors (along outgoing edges).
            if (node.getAdjacenyList().size() > 0) {
                // Each neighbor gets an equal share of PageRank mass.
                ArrayListOfIntsWritable list = node.getAdjacenyList();
                float mass = node.getPageRank() - (float) StrictMath.log(list.size());

                // Iterate over neighbors.
                for (int i = 0; i < list.size(); i++) {
                    int neighbor = list.get(i);

                    if (map.containsKey(neighbor)) {
                        // Already message destined for that node; add PageRank mass contribution.
                        massMessagesSaved++;
                        map.put(neighbor, sumLogProbs(map.get(neighbor), mass));
                    } else {
                        // New destination node; add new entry in map.
                        massMessages++;
                        map.put(neighbor, mass);
                    }
                }
            }
        }

        //@Override
        public void cleanup(Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context)
                throws IOException, InterruptedException {
            // Now emit the messages all at once.
            IntWritable k = new IntWritable();
            PageRankNode mass = new PageRankNode();

            for (MapIF.Entry e : map.entrySet()) {
                k.set(e.getKey());

                mass.setNodeId(e.getKey());
                mass.setType(PageRankNode.Type.Mass);
                mass.setPageRank(e.getValue());

                context.write(k, mass);
            }
        }
    }

    // Combiner: sums partial PageRank contributions and passes node structure along.
    public static class CombineClass extends Combiner<IntWritable, PageRankNode, IntWritable, PageRankNode> {

        private static final PageRankNode intermediateMass = new PageRankNode();

        @Override
        public void reduce(IntWritable nid, Iterable<PageRankNode> values, Combiner.Context context)
                throws IOException, InterruptedException {

            int massMessages = 0;

            // Remember, PageRank mass is stored as a log prob.
            float mass = Float.NEGATIVE_INFINITY;
            for (PageRankNode n : values) {
                if (n.getType() == PageRankNode.Type.Structure) {
                    // Simply pass along node structure.
                    context.write(nid, n);
                } else {
                    // Accumulate PageRank mass contributions.
                    mass = sumLogProbs(mass, n.getPageRank());
                    massMessages++;
                }
            }

            // Emit aggregated results.
            if (massMessages > 0) {
                intermediateMass.setNodeId(nid.get());
                intermediateMass.setType(PageRankNode.Type.Mass);
                intermediateMass.setPageRank(mass);

                context.write(nid, intermediateMass);
            }
        }
    }

    // Reduce: sums incoming PageRank contributions, rewrite graph structure.
    public static class ReduceClass extends Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {

        // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
        // through dangling nodes.
        private float totalMass = Float.NEGATIVE_INFINITY;

        @Override
        public void reduce(IntWritable nid, Iterable<PageRankNode> iterable, Context context)
                throws IOException, InterruptedException {

            Iterator<PageRankNode> values = iterable.iterator();

            // Create the node structure that we're going to assemble back together from shuffled pieces.
            PageRankNode node = new PageRankNode();

            node.setType(PageRankNode.Type.Complete);
            node.setNodeId(nid.get());

            int massMessagesReceived = 0;
            int structureReceived = 0;

            float mass = Float.NEGATIVE_INFINITY;
            while (values.hasNext()) {
                PageRankNode n = values.next();

                if (n.getType().equals(PageRankNode.Type.Structure)) {
                    // This is the structure; update accordingly.
                    ArrayListOfIntsWritable list = n.getAdjacenyList();
                    structureReceived++;

                    node.setAdjacencyList(list);
                } else {
                    // This is a message that contains PageRank mass; accumulate.
                    mass = sumLogProbs(mass, n.getPageRank());
                    massMessagesReceived++;
                    totalMass = sumLogProbs(totalMass, n.getPageRank());
                }
            }

            // Update the final accumulated PageRank mass.
            node.setPageRank(mass);

            // Error checking.
            if (structureReceived == 1) {
                // Everything checks out, emit final node structure with updated PageRank value.
                context.write(nid, node);

                // Keep track of total PageRank mass.
                totalMass = sumLogProbs(totalMass, mass);
            } else if (structureReceived == 0) {
                // We get into this situation if there exists an edge pointing to a node which has no
                // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
                // log and count but move on.
                //        context.getCounter(PageRank.missingStructure).increment(1);
                System.out.println("No structure received for nodeid: " + nid.get() + " mass: " + massMessagesReceived);
            } else {
                // This shouldn't happen!
                throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
                        + " mass: " + massMessagesReceived + " struct: " + structureReceived);
            }
        }

        @Override
        public void cleanup(
                Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context)
                throws IOException {

            Configuration conf = context.getConfiguration();
            //String taskId = conf.get("mapred.task.id");
            String path = conf.get("PageRankMassPath");
            //System.out.println("Page rank mask path" + path);


            //Preconditions.checkNotNull(taskId);
            taskId++;
            Preconditions.checkNotNull(path);

            ArrayList<Float> listMass;
            if (massMap.containsKey(path)) {
                listMass = massMap.get(path);
                listMass.add(totalMass);
                massMap.put(path, listMass);
            } else {
                listMass = new ArrayList<Float>();
                listMass.add(totalMass);
                massMap.put(path, listMass);
            }

            // Write to a file the amount of PageRank mass we've seen in this reducer.
//            boolean success = (new File(path)).mkdirs();
//            BufferedWriter out = new BufferedWriter(new FileWriter(path + "/" + taskId));
//            //sys.out.println(totalMass);
//            out.write(Float.toString(totalMass));
//            out.close();
        }
    }

    // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
    // of the random jump factor.
    public static class MapPageRankMassDistributionClass extends Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

        private float missingMass = 0.0f;
        private int nodeCnt = 0;

        @Override
        public void setup(Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context)
                throws IOException {
            Configuration conf = context.getConfiguration();

            missingMass = conf.getFloat("MissingMass", 0.0f);
            nodeCnt = conf.getInt("NodeCount", 0);
        }

        @Override
        public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
                InterruptedException {

            float p = node.getPageRank();

            float jump = (float) (Math.log(ALPHA) - Math.log(nodeCnt));
            float link = (float) Math.log(1.0f - ALPHA)
                    + sumLogProbs(p, (float) (Math.log(missingMass) - Math.log(nodeCnt)));

            p = sumLogProbs(jump, link);
            node.setPageRank(p);

            context.write(nid, node);
        }
    }
    // Random jump factor.
    private static float ALPHA = 0.15f;
    private static NumberFormat formatter = new DecimalFormat("0000");

    /**
     * Dispatches command-line arguments to the tool via the
     * <code>ToolRunner</code>.
     */
    public static void main(String[] args) throws Exception {
        //int res = ToolRunner.run(new Configuration(), new RunPageRankBasic(), args);
        massMap = new HashMap<String, ArrayList<Float>>();
        RunPageRankBasic thisJob = new RunPageRankBasic();
        System.exit(thisJob.run(args));
        //System.exit(res);
    }

    public RunPageRankBasic() {
    }

    private static int printUsage() {
        System.out.println("usage: [basePath] [numNodes] [start] [end] [useCombiner?] [useInMapCombiner?] [useRange?]");
        //ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }
    private int endIter;

    /**
     * Runs this tool.
     */
    public int run(String[] args) throws Exception {

//		if (args.length != 7) {
//			printUsage();
//			return -1;
//		}

//		String basePath = args[0];
//		int n = Integer.parseInt(args[1]);
//		int s = Integer.parseInt(args[2]);
//		int e = Integer.parseInt(args[3]);
//		boolean useCombiner = Integer.parseInt(args[4]) != 0;
//		boolean useInmapCombiner = Integer.parseInt(args[5]) != 0;
//		boolean useRange = Integer.parseInt(args[6]) != 0;


        String basePath = "data/pagerank-temp";
        int n = 10;
        int s = 0;
        int e = 10;
        boolean useCombiner = false;
        boolean useInmapCombiner = false;
        boolean useRange = false;

        endIter = 10;

        // Iterate PageRank.
        for (int i = s; i < e; i++) {
            iteratePageRank(i, i + 1, basePath, n, useCombiner, useInmapCombiner);
        }

        return 0;
    }
    private List<Future<Object>> reduceOutput;

    // Run each iteration.
    private void iteratePageRank(int i, int j, String basePath, int numNodes, boolean useCombiner, boolean useInMapperCombiner) throws Exception {
        // Each iteration consists of two phases (two MapReduce jobs).

        // Job 1: distribute PageRank mass along outgoing edges.
        Object[] objs = phase1(i, j, basePath, numNodes, useCombiner, useInMapperCombiner);
        float mass = (Float) objs[0];
        int numMappers = (Integer) objs[1];

        // Find out how much PageRank mass got lost at the dangling nodes.
        float missing = 1.0f - (float) StrictMath.exp(mass);
//        if (missing < 0.0f) {
//            missing = 0.0f;
//        }

        // Job 2: distribute missing mass, take care of random jump factor.
        phase2(i, j, missing, basePath, numNodes, numMappers);
    }

    private Object[] phase1(int i, int j, String basePath, int numNodes, boolean useCombiner, boolean useInMapperCombiner) throws Exception {
        System.out.println("Phase 1 -- Iteration: " + i);

        Job job = new Job(new Configuration(), "PageRank:Basic:iteration" + j + ":Phase1");
        job.setMapperClass(RunPageRankBasic.MapClass.class);
        job.setReducerClass(RunPageRankBasic.ReduceClass.class);

        String in = basePath + "/iter" + formatter.format(i);
        System.out.println(in);
        //String in = basePath;
        String out = basePath + "/iter" + formatter.format(j) + "t";
        String outm = out + "-mass";

        File outdir = new File(out);
        outdir.mkdirs();

        // We need to actually count the number of part files to get the number of partitions (because
        // the directory might contain _log).
        int numPartitions = 0;
        //get to this soon

        File dir = new File(in);
        if (i == 0) {
            for (File child : dir.listFiles()) {
                if (".".equals(child.getName()) || "..".equals(child.getName())) {
                    continue;  // Ignore the self and parent aliases.
                }
                if (child.getName().toCharArray()[0] != '.' && child.getName().contains("pagerankrecords")) {
                    numPartitions++;
                }
            }
        } else {
            for (Future<Object> fut : reduceOutput) {
                numPartitions++;
            }
        }

        int numMapTasks = numPartitions;
//        int numReduceTasks = numPartitions;
        int numReduceTasks = 6;


        job.getConfiguration().setInt("NodeCount", numNodes);
        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
        job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
        job.getConfiguration().set("PageRankMassPath", outm);
        job.setFirstIterationInputFromDisk(true);
        job.setInputDirectoryPath(in);
        job.setInMemoryInput(true);
        job.setIterationNo(i);
        job.setMapInputFromReducer(true);
        job.setMapInput(reduceOutput);
        job.setMapInputKeyClass(IntWritable.class);
        job.setMapInputValueClass(PageRankNode.class);
        reduceOutput = null;
        job.setOutputFile(new File(out + "/pagerankrecords"));
        job.setNumMapTasks(numMapTasks);
        job.setNumReduceTasks(numReduceTasks);
        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out + "/pagerankrecords"));
        job.setInputFormatClass(NonSplitableSequenceMemoryInputFormat.class);
        job.setOutputFormatClass(SequenceMemoryOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PageRankNode.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PageRankNode.class);
        job.setInMemoryOutput(true);
        job.setIterationOutputToDisk(false, endIter);
        job.setPartitionerClass(HashPartitioner.class);
        job.setMapperClass(useInMapperCombiner ? MapWithInMapperCombiningClass.class : MapClass.class);
        job.setReducerRecordReader(IntPageRankNodeRecordReader.class);

        if (useCombiner) {
            job.setCombinerClass(CombineClass.class);
        }

        job.setReducerClass(ReduceClass.class);

        job.waitForCompletion(true);
        reduceOutput = job.getReduceOutput();
        int numMappers = job.getNumMapTasks();

        float mass = Float.NEGATIVE_INFINITY;

        int count = 0;
        ArrayList<Float> listMass = massMap.get(outm);
        if (listMass != null) {
            System.out.println("List Mass Size: " + listMass.size());
            for (Float massVal : listMass) {
                if (massVal != null) {
                    mass = sumLogProbs(mass, massVal);
                }
                count++;
            }
        }
        System.out.println("Mass: " + mass + " Count: " + count);

        Object[] objs = new Object[2];
        objs[0] = mass;
        objs[1] = numMappers;
        
        return objs;
    }

    private void phase2(int i, int j, float missing, String basePath, int numNodes, int numMappers) throws Exception {
        System.out.println("Phase 2 -- Iteration: " + i);
        Job job = new Job(new Configuration(), "PageRank:Basic:iteration" + j + ":Phase2");
        //job.setJarByClass(RunPageRankBasic.class);

        String in = basePath + "/iter" + formatter.format(j) + "t";
        System.out.println(in);
        String out = basePath + "/iter" + formatter.format(j);

        File outdir = new File(out);
        outdir.mkdirs();
        
        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
        job.getConfiguration().setFloat("MissingMass", (float) missing);
        job.getConfiguration().setInt("NodeCount", numNodes);
        job.setReduceOutput(reduceOutput);
        reduceOutput = null;
        job.setFirstIterationInputFromDisk(false);
        job.setInMemoryInput(true);
        job.setIterationNo(i);
        job.setMapInputFromReducer(true);
        job.setNumMapTasks(numMappers);
        job.setInputDirectoryPath(in);
        job.setOutputFile(new File(out + "/pagerankrecords"));
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out + "/pagerankrecords"));
        job.setInputFormatClass(NonSplitableSequenceMemoryInputFormat.class);
        job.setOutputFormatClass(SequenceMemoryOutputFormat.class);
        job.setMapInputKeyClass(IntWritable.class);
        job.setMapInputValueClass(PageRankNode.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PageRankNode.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PageRankNode.class);
        job.setMapperClass(MapPageRankMassDistributionClass.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setInMemoryOutput(true);
        job.setIterationOutputToDisk(true, endIter);
        job.waitForCompletion(true);
        job.setReducerRecordReader(IntPageRankNodeRecordReader.class);
        reduceOutput = job.getReduceOutput();
    }

    // Adds two log probs.
    private static float sumLogProbs(float a, float b) {
        if (a == Float.NEGATIVE_INFINITY) {
            return b;
        }

        if (b == Float.NEGATIVE_INFINITY) {
            return a;
        }

        if (a < b) {
            return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
        }

        return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
    }
}
