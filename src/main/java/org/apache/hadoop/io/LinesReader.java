/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.io;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ashwinkayyoor
 */
public class LinesReader {
    
    private static final Properties prop;
    
    static{
        prop = new Properties();
        try {
            prop.load(new FileInputStream("config.properties"));
        } catch (IOException ex) {
            Logger.getLogger(LinesReader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public ArrayList getAllTextLines(List<Future< Object>> invokeAll) throws InterruptedException, ExecutionException, FileNotFoundException, IOException {

        //Properties prop = new Properties();        
        ThreadPoolExecutor executorService1 = (ThreadPoolExecutor) Executors.newFixedThreadPool(Integer.parseInt(prop.getProperty("poolThreads1")));
        final List<Callable<Object>> parseTasks = new ArrayList<Callable<Object>>();

        for (Future< Object> fut : invokeAll) {
            //System.out.println("Start");
            ByteBuffer byteBuffer = (ByteBuffer) fut.get();
            //System.out.println(byteBuffer.capacity());
            parseTasks.add(new ParseBuffers(byteBuffer));
            //System.out.println("array length: "+byteBuffer.capacity());
        }

        List<Future<Object>> invokeAllTasks = executorService1.invokeAll(parseTasks);
        executorService1.shutdown();

        ArrayList linesListsList = new ArrayList();
        for (Future< Object> fut : invokeAllTasks) {
            
            ArrayList linesList = (ArrayList) fut.get();
            linesListsList.add(linesList);
        
        }

        return linesListsList;
    }
}
