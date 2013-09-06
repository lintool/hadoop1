package test.mapreduce4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.hadoop.mapreduce.lib.output.KVPRecordReader;
import static org.junit.Assert.*;
import org.junit.Test;
 
public class KVPRecordReaderTest {

	@Test
	public void testAll() {
		try {
			KVPRecordReader kvp1 = new KVPRecordReader();
			InitialiseReader(kvp1, "kvp1.txt");
			
			/*
			a	The cat sat on the mat
			a	The cat sat on the mat
			a	The cat sat on the mat
			a	The cat sat on the mat
			b	The dog bit the cat
			b	The dog bit the cat
			d	The dog ate the rabbit			
			*/
			assertTrue(kvp1.nextKeyValue());
			assertEquals("a", kvp1.getCurrentKey().toString());
			assertEquals("The cat sat on the mat", kvp1.getCurrentValue().toString());
			assertTrue(kvp1.nextKeyValue());
			assertEquals("a", kvp1.getCurrentKey().toString());
			assertEquals("The cat sat on the mat", kvp1.getCurrentValue().toString());
			assertTrue(kvp1.nextKeyValue());
			assertEquals("a", kvp1.getCurrentKey().toString());
			assertEquals("The cat sat on the mat", kvp1.getCurrentValue().toString());
			assertTrue(kvp1.nextKeyValue());
			assertEquals("a", kvp1.getCurrentKey().toString());
			assertEquals("The cat sat on the mat", kvp1.getCurrentValue().toString());
			assertTrue(kvp1.nextKeyValue());
			assertEquals("b", kvp1.getCurrentKey().toString());
			assertEquals("The dog bit the cat", kvp1.getCurrentValue().toString());
			assertTrue(kvp1.nextKeyValue());
			assertEquals("b", kvp1.getCurrentKey().toString());
			assertEquals("The dog bit the cat", kvp1.getCurrentValue().toString());
			assertTrue(kvp1.nextKeyValue());
			assertEquals("d", kvp1.getCurrentKey().toString());
			assertEquals("The dog ate the rabbit", kvp1.getCurrentValue().toString());
			assertFalse(kvp1.nextKeyValue());
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
        
        private void test(){
            try {
			KVPRecordReader kvp1 = new KVPRecordReader();
			InitialiseReader(kvp1, "kvp1.txt");
			
			/*
			a	The cat sat on the mat
			a	The cat sat on the mat
			a	The cat sat on the mat
			a	The cat sat on the mat
			b	The dog bit the cat
			b	The dog bit the cat
			d	The dog ate the rabbit			
			*/
                        kvp1.nextKeyValue();;
			if(kvp1.getCurrentValue().toString().equals("a"))
                        assertEquals("a", kvp1.getCurrentKey().toString());
			assertEquals("The cat sat on the mat", kvp1.getCurrentValue().toString());
			assertTrue(kvp1.nextKeyValue());
			assertEquals("a", kvp1.getCurrentKey().toString());
			assertEquals("The cat sat on the mat", kvp1.getCurrentValue().toString());
			assertTrue(kvp1.nextKeyValue());
			assertEquals("a", kvp1.getCurrentKey().toString());
			assertEquals("The cat sat on the mat", kvp1.getCurrentValue().toString());
			assertTrue(kvp1.nextKeyValue());
			assertEquals("a", kvp1.getCurrentKey().toString());
			assertEquals("The cat sat on the mat", kvp1.getCurrentValue().toString());
			assertTrue(kvp1.nextKeyValue());
			assertEquals("b", kvp1.getCurrentKey().toString());
			assertEquals("The dog bit the cat", kvp1.getCurrentValue().toString());
			assertTrue(kvp1.nextKeyValue());
			assertEquals("b", kvp1.getCurrentKey().toString());
			assertEquals("The dog bit the cat", kvp1.getCurrentValue().toString());
			assertTrue(kvp1.nextKeyValue());
			assertEquals("d", kvp1.getCurrentKey().toString());
			assertEquals("The dog ate the rabbit", kvp1.getCurrentValue().toString());
			assertFalse(kvp1.nextKeyValue());
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
        }

	private void InitialiseReader(KVPRecordReader kvp1, String filename)
			throws URISyntaxException, FileNotFoundException, IOException {
		File input = new File(this.getClass().getResource(filename).toURI());
		FileInputStream fis=new FileInputStream(input);
		FileChannel fcin=fis.getChannel();
		ByteBuffer buffer = ByteBuffer.allocate((int)fcin.size());
		fcin.read(buffer);
		buffer.rewind();
		kvp1.initialize(buffer);
	}

}
