package mapreduce4j;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.nio.channels.FileChannel;

import org.junit.*;

public class TextInputFormatTest {

	@Test
	public void testBytesUntilRecord() {
		try {
			File input = new File(TextInputFormatTest.class.getResource("test.tab").toURI());
			FileInputStream fis=new FileInputStream(input);
			FileChannel fcin=fis.getChannel();
			TextInputFormat tif = new TextInputFormat();
			// first line is 22 chars, but it reads the split char also
			assertEquals(23, tif.bytesUntilRecord(fcin, 0));
			// second line is 26 chars, but it reads the split char also
			assertEquals(27, tif.bytesUntilRecord(fcin, 23));
			fcin.close();
			fis.close();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
