package edu.mayo.pipes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

public class InputStreamPipeTest {

	@Test
	public void testProcessNextStart() throws FileNotFoundException {

		InputStreamPipe isp = new InputStreamPipe();

		// data to feed into stream
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("line1");
		pw.println("line2");
		pw.println(); // line 3 is blank
		pw.println("line4");
		InputStream inStream = new ByteArrayInputStream(sw.toString().getBytes());

		Pipe<InputStream, String> pipeline = new Pipeline<InputStream, String>(isp);
		pipeline.setStarts(Arrays.asList(inStream));

		// grab 1st row of data
		assertTrue(pipeline.hasNext());
		assertEquals("line1", pipeline.next());
		assertTrue(pipeline.hasNext());
		assertEquals("line2", pipeline.next());
		assertTrue(pipeline.hasNext());
		assertEquals("", pipeline.next());
		assertTrue(pipeline.hasNext());
		assertEquals("line4", pipeline.next());
		assertFalse(pipeline.hasNext());
	}

}
