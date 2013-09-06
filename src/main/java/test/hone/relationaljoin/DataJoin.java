/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test.hone.relationaljoin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DynamicDirectByteBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author ashwinkayyoor
 */
public class DataJoin {

    public static class MapClass extends DataJoinMapperBase {

        protected Text generateInputTag(String inputFile) {
            String datasource = inputFile.split("-")[0];
            return new Text(datasource);
        }

        protected Text generateGroupKey(TaggedMapOutput aRecord) {
            String line = ((Text) aRecord.getData()).toString();
            String[] tokens = line.split(",");
            String groupKey = tokens[0];
            return new Text(groupKey);
        }

        protected TaggedMapOutput generateTaggedMapOutput(Object value) {
            TaggedWritable retv = new TaggedWritable((Text) value);
            retv.setTag(this.inputTag);
            return retv;
        }
    }

    public static class Reduce extends DataJoinReducerBase {

        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
            if (tags.length < 2) {
                return null;
            }
            String joinedStr = "";
            for (int i = 0; i < values.length; i++) {
                if (i > 0) {
                    joinedStr += ",";
                }
                TaggedWritable tw = (TaggedWritable) values[i];
                String line = ((Text) tw.getData()).toString();
                String[] tokens = line.split(",", 2);
                joinedStr += tokens[1];
            }
            TaggedWritable retv = new TaggedWritable(new Text(joinedStr));
            retv.setTag((Text) tags[0]);
            return retv;
        }
    }

    public static class TaggedWritable extends TaggedMapOutput {

        private WritableComparable data;

        public TaggedWritable(WritableComparable data) {
            this.tag = new Text("");
            this.data = data;
        }

        public WritableComparable getData() {
            return data;
        }

        @Override
        public int write(DataOutputStream out) throws IOException {
            this.tag.write(out);
            this.data.write(out);
            return 0;
        }

        public void readFields(DataInputStream in) throws IOException {
            this.tag.readFields(in);
            this.data.readFields(in);
        }

        @Override
        public int compare(byte[] a, int i1, int j1) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getOffset() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void set(WritableComparable obj) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int compareTo(Object o) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int write(ByteBuffer buf) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int write(DynamicDirectByteBuffer buf) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void readFields(byte[] input, int offset) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public WritableComparable create(byte[] input, int offset) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
//        Path in = new Path(args[0]);
//        Path out = new Path(args[1]);
//        FileInputFormat.setInputPaths(job, in);
//        FileOutputFormat.setOutputPath(job, out);
//        job.setJobName("DataJoin");
//        job.setMapperClass(MapClass.class);
//        job.setReducerClass(Reduce.class);
//        job.setInputFormat(TextInputFormat.class);
//        job.setOutputFormat(TextOutputFormat.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(TaggedWritable.class);
//        job.set("mapred.textoutputformat.separator", ",");
//        JobClient.runJob(job);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        //int res = ToolRunner.run(new Configuration(),                new DataJoin(),args);
        System.exit(-1);
    }
}