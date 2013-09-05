/**
 *
 */
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author tim
 *
 */
public class CallableMapper implements Callable {

    private final Mapper mapper;
    private final Mapper.Context context;
    private List inputList;
    private Object[] returnData = new Object[3];
    private Properties prop;
    private int id;
    private Object[] intermArray;
    private final static int ARCH_1 = 1, ARCH_2 = 2, ARCH_3 = 3;

    public CallableMapper(final Mapper mapper, final Mapper.Context context, final List inputList) {
        this.mapper = mapper;
        this.context = context;
        this.inputList = inputList;
    }

    public CallableMapper(final Mapper mapper, final Mapper.Context context, final int id, final Properties prop, Object[] intermArray) {
        this.mapper = mapper;
        this.context = context;
        this.id = id;
        this.prop = prop;
        this.intermArray = intermArray;
    }

    /**
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public final Object call() throws IOException, InterruptedException, NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        try {
            int arch = Integer.parseInt(prop.getProperty("arch"));
            try {
                mapper.run(context, id, prop);
            } catch (Throwable ex) {
                Logger.getLogger(CallableMapper.class.getName()).log(Level.SEVERE, null, ex);
            }

            if (arch == ARCH_1) {
                returnData[0] = mapper.getData();
                returnData[1] = mapper.getOffsets();
            } else if (arch == ARCH_2) {
//                returnData[0] = mapper.getDataArray();
//                returnData[1] = mapper.getOffsetsArray();
//                returnData[2] = mapper.getElemCount();
                intermArray[this.id * 3 + 0] = mapper.getDataArray();
                intermArray[this.id * 3 + 1] = mapper.getOffsetsArray();
                intermArray[this.id * 3 + 2] = mapper.getElemCount();
            } else if (arch == ARCH_3) {
            }

            return null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    /*
     * @Override public Object call() throws Exception { try { LinkedList data =
     * mapper.run(inputList, context); return data; } catch (Exception e) { }
     * return null; }
     */
}
