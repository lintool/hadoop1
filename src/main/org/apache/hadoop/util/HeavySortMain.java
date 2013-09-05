package org.apache.hadoop.util;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.util.HeavySort.ArrayFactory;

public class HeavySortMain {
	
	public static void main(String [] args){
		int  problemSize = 200000000;
		int numThreads = 6;
		Random random = new Random();
		Integer[] sortArray = new Integer[problemSize];
		for (int i=0; i< problemSize ;i++){
			sortArray[i] = random.nextInt(Integer.MAX_VALUE );
		}
		/*long startTime2 = System.currentTimeMillis();
		Arrays.sort(sortArray);
		System.out.println("Single Threaded Sort: time taken " + 
				(System.currentTimeMillis() - startTime2));
		Collections.shuffle(Arrays.asList(sortArray));*/
		final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		long startTime = System.currentTimeMillis();
		ArrayFactory<Integer> factory = new ArrayFactory<Integer>(){

			@Override
			public Integer[] buildArray(int length) {
				return new Integer[length];
			}

		};
		HeavySort.sort(sortArray,executor,numThreads,factory);
		System.out.println("Multi-Threaded sort: time taken " + 
				(System.currentTimeMillis() - startTime));
		

		for (int i=0; i<sortArray.length-1; i++){
			if(sortArray[i] > sortArray[i+1]){
				System.err.println("Error: element at " + i 
						+ " : " + sortArray[i]  );
				System.err.println("Error: element at " + 
						(i+1) + " : " + sortArray[i+1]  );
			}

		}
		executor.shutdown();
	}

}
