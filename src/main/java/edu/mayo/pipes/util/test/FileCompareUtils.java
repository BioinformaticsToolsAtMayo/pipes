package edu.mayo.pipes.util.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.tinkerpop.pipes.Pipe;

import edu.mayo.pipes.history.History;

/** Put this class in the main java packages so it can be picked up an used by other projects 
 *  such as bior_pipeline and bior_catalog  */
public class FileCompareUtils {
	
	public static final String EOL = System.getProperty("line.separator");
	
	/** Use the JUnit asserts to validate if two files have the same size and content */
	public static void assertFileEquals(String fileExpected, String fileActual) throws IOException {
		List<String> linesExpected = Files.readLines(new File(fileExpected), Charset.forName("UTF-8"));
		List<String> linesActual   = Files.readLines(new File(fileActual), Charset.forName("UTF-8"));
		
		PipeTestUtils.assertListsEqual(linesExpected, linesActual);
		
		// Verify that the number of lines in the input equals the # of lines in the output catalog (one variant in to each variant out)
		assertEquals( "There should be the same number of variants in the output as were in the input.  ",
				Files.readLines(new File(fileExpected), Charset.forName("UTF-8")).size(),
				Files.readLines(new File(fileActual), Charset.forName("UTF-8")).size() );
	}

	public static void saveToFile(ArrayList<String> lines, String filePath) throws IOException {
		StringBuilder str = new StringBuilder();
		for(String line : lines) {
			str.append(line);
			str.append(EOL);
		}
		Files.write(str.toString().getBytes(), new File(filePath));
	}
	
	public static List<String> loadFile(String filePath) throws IOException {
		return Files.readLines(new File(filePath), Charsets.UTF_8);
	}
}
