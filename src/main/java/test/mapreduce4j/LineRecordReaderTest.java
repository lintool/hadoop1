package mapreduce4j;

import org.apache.hadoop.mapreduce.lib.output.LineRecordReader;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.Test;

public class LineRecordReaderTest {

	@Test
	public void testNextKeyValue() {
		try {
			File input = new File(TextInputFormatTest.class.getResource("test.tab").toURI());
			FileInputStream fis=new FileInputStream(input);
			FileChannel fcin=fis.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate((int)fcin.size());
			fcin.read(buffer);
			buffer.rewind();
			LineRecordReader lrr = new LineRecordReader();
			lrr.initialize(buffer);
			
			assertTrue(lrr.nextKeyValue());
			assertEquals(1, lrr.getCurrentKey().get());
			assertEquals("The	cat	sat	on	the	mat", lrr.getCurrentValue().toString());

			assertTrue(lrr.nextKeyValue());
			assertEquals(2, lrr.getCurrentKey().get());
			assertEquals("The	dog	chased	the	cat	who", lrr.getCurrentValue().toString());
			
			assertTrue(lrr.nextKeyValue());
			assertEquals(3, lrr.getCurrentKey().get());
			assertEquals("sat	on	the	mat	for	fun", lrr.getCurrentValue().toString());
			
			assertFalse(lrr.nextKeyValue());
			
			
			fcin.close();
			fis.close();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
