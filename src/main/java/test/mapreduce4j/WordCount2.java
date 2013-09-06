/**
 *
 */
package test.mapreduce4j;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 *
 * @author tim
 */
public class WordCount2 {

    /**
     * Tokens the input line and emits the word and one as the value
     */
    public static class WordTokenizer extends Mapper<LongWritable, Text, Text, IntWritable> {

        static Pattern tab = Pattern.compile(" ");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // just to illustrate getting something out of the context
//            String name = context.getConfiguration().get("name");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.setStatus("Key[" + key.get() + "], Value[" + value.toString() + "]");
            String[] parts = tab.split(value.toString());
            for (String s : parts) {
                context.write(new Text(s), new IntWritable(1));
            }
        }
    }

    /**
     * Takes the words as keys and counts them
     */
    public static class SumWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable v : values) {
                total += v.get();
            }
            context.setStatus("Key[" + key.toString() + "], had total summed count[" + total + "]");
            context.write(key, new IntWritable(total));
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // just for illustration that the setup is called once
            super.setup(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // just for illustration that a cleanup is called
            super.cleanup(context);
        }
    }

    /**
     * @param args <in> <out> <mapping-file> <table-name>
     */
    public static void main(String[] args) throws Exception {
        Logger.global.setLevel(Level.OFF);
        Configuration conf = new Configuration();

        // the following is NOT a Hadoop job but a MapReduce4J job
        // this would need replaced to run on a Hadoop cluster
        Job job = new Job(conf, "WordCount");
        job.setMapperClass(WordTokenizer.class);
        job.setReducerClass(SumWordCount.class);

        // a job input can span multiple files
        job.addInput(new File("data/datafile"));
        // jut add the file like so
        //job.addInput(new File(args[0]));
        //job.addInput(new File(args[0]));

        // working on simple tab delimited files
        job.setInputFormatClass(TextInputFormat.class);

        // There is no output so perhaps it can be ignored?
        //job.setMapOutputKeyClass(Text1.class);
        //job.setMapOutputValueClass(Text1.class);
        //job.setOutputKeyClass(Text1.class);
        //job.setOutputValueClass(Text1.class);



        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
