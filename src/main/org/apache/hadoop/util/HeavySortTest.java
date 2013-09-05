package org.apache.hadoop.util;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.util.HeavySort.ArrayFactory;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Test;

public class HeavySortTest {
	
	private static ArrayFactory<Integer> integerArrayFactory = 
		new ArrayFactory<Integer>(){

		@Override
		public Integer[] buildArray(int length) {
			return new Integer[length];
		}
    	
    };
    
    private static Integer[] zeroArray(){
    	return new Integer[0];
    }
    
    private static Integer[] oneArray(){
    	return new Integer[]{Integer.valueOf(-12)};
    }
    
    private static Integer[] twoArray(){
    	return new Integer[]{Integer.valueOf(300), Integer.valueOf(100)};
    }
    
    private static Integer[] threeArray(){
    	return new Integer[]{Integer.valueOf(-10), 
    			Integer.valueOf(-5), Integer.valueOf(-1)};
    }    
    
    private static Integer[] nineArray(){
    	return new Integer[]{14, 4 ,100,140,-4,8,30,4,-20 };
    }
    
    

	@Test
	public void test() {
		for(int i : new int[]{1,3,4,5,8,10}){
			ExecutorService exec = Executors.newFixedThreadPool(i);
			{
				Integer[] zero = zeroArray();
				HeavySort.sort(zero, exec, i, integerArrayFactory);
				Integer[] ans = zeroArray();
				Arrays.sort(ans);
				assertArrayEquals(ans,zero);
			}
			{
				Integer[] one = oneArray();
				HeavySort.sort(one, exec, i, integerArrayFactory);
				Integer[] ans = oneArray();
				Arrays.sort(ans);
				assertArrayEquals(ans,one);
			}
			{
				Integer[] two = twoArray();
				HeavySort.sort(two, exec, i, integerArrayFactory);
				Integer[] ans = twoArray();
				Arrays.sort(ans);
				assertArrayEquals(ans,two);
			}
			{
				Integer[] three = threeArray();
				HeavySort.sort(three, exec, i, integerArrayFactory);
				Integer[] ans = threeArray();
				Arrays.sort(ans);
				assertArrayEquals(ans,three);
			}
			{
				Integer[] nine = nineArray();
				HeavySort.sort(nine, exec, i, integerArrayFactory);
				Integer[] ans = nineArray();
				Arrays.sort(ans);
				assertArrayEquals(ans,nine);
			}
		}
	}

}
