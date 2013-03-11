package edu.mayo.pipes.util.exec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
		
		List<String> inLines = Arrays.asList("foo", "bar"); 
		p.send(inLines);
				
		ProcessOutput output = p.close();
		assertEquals(0, output.exitCode);
		assertEquals(0, output.stderr.length());
		
		List<String> outLines = p.receive();
		assertEquals(2, outLines.size());
		assertEquals("foo", outLines.get(0));
		assertEquals("bar", outLines.get(1));		
	}
	@Test
	/**
	 * Tests a valid command such as cat with a single argument.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void testCommandWithSingleArg() throws IOException, InterruptedException {
		String command = "cat";
		String[] commandArgs = {"-n"}; //number all output lines
		boolean useParentEnv = true;
		StreamingCommandProcess p = 
				new StreamingCommandProcess(command, commandArgs, NO_CUSTOM_ENV, useParentEnv);

		p.start();
		
		List<String> inLines = Arrays.asList("foobar", "test"); 
		p.send(inLines);
	
		ProcessOutput output = p.close();
		assertEquals(0, output.exitCode);
		assertEquals(0, output.stderr.length());

		List<String> outLines = p.receive();
		
		assertEquals(2, outLines.size());
		assertEquals("1\tfoobar", outLines.get(0).trim());		
		assertEquals("2\ttest", outLines.get(1).trim());		
	}	
	
	@Test
	/**
	 * Tests custom environment variables using the /bin/echo command.
	 */
	public void testCustomEnvironment() throws IOException, InterruptedException {
		
		// need to run /bin/echo within a shell so that $MYVAR is resolved
		String command = "/bin/sh";
		String[] commandArgs = {"-c", "/bin/echo $MYVAR"};
		
		Map<String, String> customEnv = new HashMap<String, String>();
		customEnv.put("MYVAR", "ABC123");
		boolean useParentEnv = true;
		StreamingCommandProcess p = 
				new StreamingCommandProcess(command, commandArgs, customEnv, useParentEnv);

		p.start();
	
		ProcessOutput output = p.close();
		assertEquals(0, output.exitCode);
		assertEquals(0, output.stderr.length());

		List<String> outLines = p.receive();
		
		assertEquals(1, outLines.size());
		assertEquals("ABC123", outLines.get(0).trim());		
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
			assertTrue(e.getMessage().startsWith("Cannot run program \"foobar\":"));
		}
	}
}
