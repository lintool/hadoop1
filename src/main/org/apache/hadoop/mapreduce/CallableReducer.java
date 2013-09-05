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
public class CallableReducer implements Callable {

    private final Reducer reducer;
    private final Reducer.Context context;
    private final Properties prop;
     private Object returnData;
   

    public CallableReducer(final Reducer reducer, final Reducer.Context context, final Properties prop) {
        this.reducer = reducer;
        this.context = context;
        this.prop = prop;
    }

    /**
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public final Object call() {
        try {
            reducer.run(context, prop);
            returnData = reducer.getOutputByteBuffer();
            return returnData;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
