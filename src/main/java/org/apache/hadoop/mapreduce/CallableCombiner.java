/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.mapreduce;

import java.util.Properties;
import java.util.concurrent.Callable;

/**
 *
 * @author ashwinkayyoor
 */
public class CallableCombiner implements Callable {

    private final Combiner combiner;
    private final Combiner.Context context;
    private final Properties prop;
    private Object[] returnData = new Object[2];
    private int id;
    private Object[][] intermArray;

    public CallableCombiner(final Combiner combiner, final Combiner.Context context, final Properties prop, Object[][] intermArray, int id) {
        this.combiner = combiner;
        this.context = context;
        this.prop = prop;
        this.id = id;
        this.intermArray = intermArray;
    }

    /**
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public final Object call() {
        try {
            combiner.run(context, prop);
            //returnData[0] = combiner.getDataArray();
            //returnData[1] = combiner.getOffsetsArray();

            intermArray[this.id][0] = combiner.getDataArray();
            intermArray[this.id][1] = combiner.getOffsetsArray();

            return returnData;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
