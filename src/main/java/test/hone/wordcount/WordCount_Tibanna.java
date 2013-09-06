package test.hone.wordcount;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Combiner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceMemoryOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextIntRecordReader;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordCount_Tibanna {

    private WordCount_Tibanna() {
    }

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        //private final StringTokenizer itr = new StringTokenizer();
        private final Text word = new Text();
        String[] tokenArray;

        //private Text1 word;
        //private HashSet data = new HashSet();
        //@Override
        @Override
        public final void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " &'=;`#<>~^@*\t\n\r\f()*-_+$!]|{}[:,.\240");

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }

//            String[] tokenArray = itr.tokenize(value.toString(), ' ');
//            int len = tokenArray.length;
//            for(int i=0;i<len;++i){
//                word.set(tokenArray[i]);
//                context.write(word, one);
//            }


//            tokenArray = StringUtils.split(value.toString());
//            final int len = tokenArray.length;
//            for (int i = 0; i < len; ++i) {
//                word.set(tokenArray[i]);
//                context.write(word, one);
//            }
            /*
             * { Text1 word = new Text1(); word.set(tokenArray[i]);
             * //data.add(word+","+one);
             *
             * context.write(word, one); // Thread.yield();
             * //System.out.println("Word: "+word+", 1"); }
             */
        }
    }

    public static class IntSumCombiner extends Combiner<Text, IntWritable, Text, IntWritable> {

        private static final IntWritable RESULT = new IntWritable();

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Combiner.Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }
            RESULT.set(sum);
            context.write(key, RESULT);
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private static final IntWritable RESULT = new IntWritable();

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;

            //System.out.println("key: " + key);
            for (IntWritable val : values) {
                sum += val.get();
            }

            RESULT.set(sum);
            context.write(key, RESULT);
        }
    }

    public static void main(final String[] args) {

        if (args.length != 2) {
            System.out.println("Args: inputDir numReducers");
        }

        final Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("config.properties"));
        } catch (IOException ex) {
            Logger.getLogger(WordCount_Tibanna.class.getName()).log(Level.SEVERE, null, ex);
        }

        //Logger.global.setLevel(Level.OFF);
        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "word count");
        //job.setJarByClass(WordCount1.class);
        job.setInputDirectoryPath(args[0]);
        job.setIterationNo(0);

        //job.setNumMapTasks(20);
        job.setNumReduceTasks(Integer.parseInt(args[1]));

        job.setMapOutputKeyClass(Text.class);

        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setOutputFormatClass(SequenceMemoryOutputFormat.class);
        job.setReducerRecordReader(TextIntRecordReader.class);

        //job.setPartitionerClass(RoundRobinPartitioner.class);


        //job.addInput(new File(prop.getProperty("inputFilePath")));
        job.setInputFormatClass(TextInputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
