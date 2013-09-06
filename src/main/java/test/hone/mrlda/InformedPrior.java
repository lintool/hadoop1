package test.hone.mrlda;

import com.google.common.base.Preconditions;
import edu.umd.cloud9.util.map.HMapIV;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ArrayListOfIntsWritable;
import org.apache.hadoop.util.DeleteFile;

public class InformedPrior {
    //static final Logger sLogger = Logger.getLogger(InformedPrior.class);

    public static final String ETA = "eta";
    public static final String INFORMED_PRIOR_OPTION = "informedprior";
    // informed prior on beta matrix
    public static final float DEFAULT_INFORMED_LOG_ETA = (float) Math.log(10.0);
    public static final float DEFAULT_UNINFORMED_LOG_ETA = (float) Math.log(0.01);

    @SuppressWarnings("unchecked")
    public int run(String[] args) throws Exception {
//        Options options = new Options();
//
//        options.addOption(Settings.HELP_OPTION, false, "print the help message");
//        options.addOption(OptionBuilder.withArgName(Settings.PATH_INDICATOR).hasArg().withDescription("input file").create(Settings.INPUT_OPTION));
//        options.addOption(OptionBuilder.withArgName(Settings.PATH_INDICATOR).hasArg().withDescription("output file").create(Settings.OUTPUT_OPTION));
        // options.addOption(OptionBuilder.withArgName(Settings.PATH_INDICATOR).hasArg().withDescription("term index file").create(ParseCorpus.INDEX));

        String termIndex = null;
        String output = null;
        String input = null;

//        CommandLineParser parser = new GnuParser();
//        HelpFormatter formatter = new HelpFormatter();
//        try {
//            CommandLine line = parser.parse(options, args);
//
//            if (line.hasOption(Settings.HELP_OPTION)) {
//                formatter.printHelp(InformedPrior.class.getName(), options);
//                System.exit(0);
//            }
//
//            if (line.hasOption(Settings.INPUT_OPTION)) {
//                input = line.getOptionValue(Settings.INPUT_OPTION);
//            } else {
//                throw new ParseException("Parsing failed due to " + Settings.INPUT_OPTION
//                        + " not initialized...");
//            }
//
//            if (line.hasOption(Settings.OUTPUT_OPTION)) {
//                output = line.getOptionValue(Settings.OUTPUT_OPTION);
//                if (output.endsWith(Path.SEPARATOR)) {
//                    output = output + ETA;
//                }
//            } else {
//                throw new ParseException("Parsing failed due to " + Settings.OUTPUT_OPTION
//                        + " not initialized...");
//            }
//
//            if (line.hasOption(ParseCorpus.INDEX)) {
//                termIndex = line.getOptionValue(ParseCorpus.INDEX);
//            } else {
//                //throw new ParseException("Parsing failed due to " + ParseCorpus.INDEX + " not initialized...");
//            }
//        } catch (ParseException pe) {
//            System.err.println(pe.getMessage());
//            formatter.printHelp(InformedPrior.class.getName(), options);
//            System.exit(0);
//        } catch (NumberFormatException nfe) {
//            System.err.println(nfe.getMessage());
//            System.exit(0);
//        }

        // Delete the output directory if it exists already
        Configuration conf = new Configuration();
        //JobConf conf = new JobConf(InformedPrior.class);
//        Job job = new Job(conf);
        //FileSystem fs = FileSystem.get(conf);

        Path inputPath = new Path(input);
        //Preconditions.checkArgument(fs.exists(inputPath) && fs.isFile(inputPath),        "Illegal input file...");

        //Path termIndexPath = new Path(termIndex);
        //Preconditions.checkArgument(fs.exists(termIndexPath) && fs.isFile(termIndexPath), "Illegal term index file...");

        Path outputPath = new Path(output);
        //fs.delete(outputPath, true);
        DeleteFile.deletefile(outputPath.toString());

        SequenceFile.Reader sequenceFileReader = null;
        SequenceFile.Writer sequenceFileWriter = null;
        BufferedReader bufferedReader = null;
//        fs.createNewFile(outputPath);
//        try {
//            bufferedReader = new BufferedReader(new InputStreamReader(new FileReader(inputPath.toString())));
//            sequenceFileReader = new SequenceFile.Reader(fs, termIndexPath, conf);
//            sequenceFileWriter = new SequenceFile.Writer(fs, conf, outputPath, IntWritable.class,
//                    ArrayListOfIntsWritable.class);
//            exportTerms(bufferedReader, sequenceFileReader, sequenceFileWriter);
//            sLogger.info("Successfully index the informed prior to " + outputPath);
//        } finally {
//            bufferedReader.close();
//            IOUtils.closeStream(sequenceFileReader);
//            IOUtils.closeStream(sequenceFileWriter);
//        }

        return 0;
    }

    public static void exportTerms(BufferedReader bufferedReader,
            SequenceFile.Reader sequenceFileReader, SequenceFile.Writer sequenceFileWriter)
            throws IOException {
        Map<String, Integer> termIndex = null;
        //= ParseCorpus.importParameter(sequenceFileReader);

        IntWritable intWritable = new IntWritable();
        ArrayListOfIntsWritable arrayListOfIntsWritable = new ArrayListOfIntsWritable();

        StringTokenizer stk = null;
        String temp = null;

        String line = bufferedReader.readLine();
        int index = 0;
        while (line != null) {
            index++;
            intWritable.set(index);
            arrayListOfIntsWritable.clear();

            stk = new StringTokenizer(line);
            while (stk.hasMoreTokens()) {
                temp = stk.nextToken();
                if (termIndex.containsKey(temp)) {
                    arrayListOfIntsWritable.add(termIndex.get(temp));
                } else {
                    //sLogger.info("How embarrassing! Term " + temp + " not found in the index file...");
                }
            }

            sequenceFileWriter.append(intWritable, arrayListOfIntsWritable);
            line = bufferedReader.readLine();
        }
    }

    public static float getEta(int termID, Set<Integer> knownTerms) {
        if (knownTerms != null && knownTerms.contains(termID)) {
            return DEFAULT_INFORMED_LOG_ETA;
        }
        return DEFAULT_UNINFORMED_LOG_ETA;
    }

    public static HMapIV<Set<Integer>> importEta(SequenceFile.Reader sequenceFileReader)
            throws IOException {
        HMapIV<Set<Integer>> lambdaMap = new HMapIV<Set<Integer>>();

        IntWritable intWritable = new IntWritable();
        ArrayListOfIntsWritable arrayListOfInts = new ArrayListOfIntsWritable();

        while (sequenceFileReader.next(intWritable, arrayListOfInts)) {
            Preconditions.checkArgument(intWritable.get() > 0, "Invalid eta prior for term "
                    + intWritable.get() + "...");

            // topic is from 1 to K
            int topicIndex = intWritable.get();
            Set<Integer> hashset = new HashSet<Integer>();

            Iterator<Integer> itr = arrayListOfInts.iterator();
            while (itr.hasNext()) {
                hashset.add(itr.next());
            }

            lambdaMap.put(topicIndex, hashset);
        }
        return lambdaMap;
    }

    public static void main(String[] args) throws Exception {
        //int res = ToolRunner.run(new Configuration(), new InformedPrior(), args);
        InformedPrior iprior = new InformedPrior();
        iprior.run(args);
        //System.exit(res);
    }
}