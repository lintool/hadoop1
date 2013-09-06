package test.hone.mrlda;

import edu.umd.cloud9.math.LogMath;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.PairOfInts;
import org.apache.hadoop.mapreduce.Combiner;

public class TermCombiner extends Combiner<PairOfInts, DoubleWritable, PairOfInts, DoubleWritable> {

    private DoubleWritable outputValue = new DoubleWritable();

    @Override
    public void reduce(PairOfInts key, Iterable<DoubleWritable> iterable, Combiner.Context context) throws IOException, InterruptedException {
        Iterator<DoubleWritable> values = iterable.iterator();

        double sum = values.next().get();
        if (key.getLeftElement() <= 0) {
            // this is not a phi value
            while (values.hasNext()) {
                sum += values.next().get();
            }
        } else {
            // this is a phi value
            while (values.hasNext()) {
                sum = LogMath.add(sum, values.next().get());
            }
        }
        outputValue.set(sum);
        context.write(key, outputValue);
    }
}