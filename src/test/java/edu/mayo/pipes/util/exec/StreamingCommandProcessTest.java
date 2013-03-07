package edu.mayo.pipes.util.exec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class StreamingCommandProcessTest {

	private static final String[] NO_ARGS = new String[0];
	private static final Map<String, String> NO_CUSTOM_ENV = new HashMap<String, String>();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	/**
	 * Tests a valid command such as cat with no arguments.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void testCommandWithNoArgs() throws IOException, InterruptedException {
		String command = "cat";
		boolean useParentEnv = true;
		StreamingCommandProcess p = 
				new StreamingCommandProcess(command, NO_ARGS, NO_CUSTOM_ENV, useParentEnv);

		p.start();
		
		List<String> inLines = Arrays.asList("foobar"); 
		p.send(inLines);

		List<String> outLines = p.receive();
		assertEquals(1, outLines.size());
		assertEquals("foobar", outLines.get(0));
		
		ProcessOutput output = p.close();
		assertEquals(0, output.exitCode);
		assertEquals(0, output.stderr.length());
	}
	
	@Test
	/**
	 * Tests what happens when the command is not valid.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void testBadCommand() throws IOException, InterruptedException {
		String command = "foobar";
		boolean useParentEnv = true;
		StreamingCommandProcess p = 
				new StreamingCommandProcess(command, NO_ARGS, NO_CUSTOM_ENV, useParentEnv);

		try {
			p.start();
			fail(String.format("Expected exception %s", IOException.class.getName()));
		} catch (IOException e) {
			String expected = "Cannot run program \"foobar\": error=2, No such file or directory"; 
			assertEquals(expected, e.getMessage());
		}
	}
}
