package edu.mayo.pipes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

public class SplitPipeTest {

	@Test
	public void test() {
		
		final String delimiter = "\t";
		SplitPipe sp = new SplitPipe(delimiter);

		String data = "A" + delimiter + "B" + delimiter + "C";
		
		Pipe<String, List<String>> pipeline = new Pipeline<String, List<String>>(sp);
		pipeline.setStarts(Arrays.asList(data));
		
		assertTrue(pipeline.hasNext());

		List<String>  list = pipeline.next();
		
		assertEquals(3, list.size());
		assertEquals("A", 	list.get(0));
		assertEquals("B", 	list.get(1));
		assertEquals("C", 	list.get(2));
		assertFalse(pipeline.hasNext());
	}

}
