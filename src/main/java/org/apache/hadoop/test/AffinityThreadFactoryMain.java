/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.test;

/**
 *
 * @author ashwinkayyoor
 */
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import vanilla.java.affinity.AffinityLock;
import static vanilla.java.affinity.AffinityStrategies.*;
import vanilla.java.affinity.AffinityThreadFactory;

/**
 * @author peter.lawrey
 */
public class AffinityThreadFactoryMain {
    private static final ExecutorService ES = Executors.newFixedThreadPool(4,
            new AffinityThreadFactory("bg", SAME_CORE, DIFFERENT_SOCKET, ANY));
       
    public static void main(String... args) throws InterruptedException {
        for (int i = 0; i < 12; i++)
            ES.submit(new Callable<Void>() {
                @Override
                public Void call() throws InterruptedException {
                    Thread.sleep(100);
                    return null;
                }
            });
        Thread.sleep(200);
        System.out.println("\nThe assignment of CPUs is\n" + AffinityLock.dumpLocks());
        ES.shutdown();
        ES.awaitTermination(1, TimeUnit.SECONDS);
    }
}