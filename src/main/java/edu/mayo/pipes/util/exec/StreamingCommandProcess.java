package edu.mayo.pipes.util.exec;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Helper class built on top of the java.lang.Runtime.exec() API to make it
 * easier to stream data TO and FROM the child process that is spawned. 
 * 
 * @see java.lang.Runtime
 * @see java.lang.Process
 * 
 * @author duffp
 *
 */
public class StreamingCommandProcess extends BaseCommandProcess {

	private static final Logger sLogger = Logger.getLogger(StreamingCommandProcess.class);
	
	// child process that is launched
	private Process mProcess;
	
	// consumes STDERR
	private Thread mStderrThread;	
	private StreamGobbler mStderrGobbler;
	
	// consumes STDOUT
	private Thread mStdoutThread;	
	private StreamGobbler mStdoutGobbler;

	// used to write data to STDIN of the child process
	private PrintWriter mStdinWriter;

	/**
	 * Constructor
	 * 
	 * NOTE: By default no shell is used, which means resolution of environment
	 * variables, back ticks ``, piping |, etc... will not work.  To get a shell 
	 * on UNIX systems, do the following:
	 * 
	 * 	<code>command="/bin/sh"</code>
	 * 	<code>commandArgs = { "-c", "/bin/echo $MYVAR" } </code>
	 * 
	 * @param command
	 * 			The command to run.  
	 * @param commandArgs
	 * 			The command arguments.
	 * @param customEnv 
	 * 			A String,String Map that contains custom environment variables.
	 * 			Note that these will overwrite variables of the same name from
	 * 			the parent environment if useParentEnv=true.
	 * @param useParentEnv
	 * 			Indicates that the parent shell environment that invoked this JVM
	 * 			should carry forward to the child process.
	 */
	public StreamingCommandProcess(
			String command,
			String[] commandArgs,
			Map<String, String> customEnv,
			boolean useParentEnv) {

		super(command, commandArgs, customEnv, useParentEnv);		
	}
	
	public void start() throws IOException {
		// start process
		mProcess = Runtime.getRuntime().exec(mCmdArray, mEnvironment);
		
		// construct a Writer that allows us to write to STDIN of the child process
		mStdinWriter = new PrintWriter(new OutputStreamWriter(mProcess.getOutputStream()));
		
		mStdoutGobbler = new StreamGobbler(mProcess.getInputStream());
		mStdoutThread = new Thread(mStdoutGobbler);
		mStdoutThread.start();
		sLogger.debug(String.format("%s will consume STDOUT", mStdoutThread.getName()));
		
		mStderrGobbler = new StreamGobbler(mProcess.getErrorStream());
		mStderrThread = new Thread(mStderrGobbler);
		mStderrThread.start();
		sLogger.debug(String.format("%s will consume STDERR", mStderrThread.getName()));
	}

	/**
	 * Sends one or more data lines to the streaming process STDIN.
	 * 
	 * @param dataLines
	 */
	public void send(List<String> dataLines){
		for (String line: dataLines) {
			mStdinWriter.println(line);
			mStdinWriter.flush();
		}
	}
	
	/**
	 * Receives zero or more data lines from the streaming process STDOUT.
	 * 
	 * @return
	 * @throws IOException
	 */
	public List<String> receive() throws IOException {
		sLogger.debug(String.format("Getting gobbled data."));
		return mStdoutGobbler.getData();
		
	}
	
	/**
	 * Sends End-of-Transmission to the process.
	 * @throws InterruptedException 
	 * @throws UnsupportedEncodingException 
	 * 
	 * @see http://en.wikipedia.org/wiki/End-of-transmission_character#Meaning_in_Unix
	 */
	public ProcessOutput close() throws InterruptedException, UnsupportedEncodingException {
		// send EOF character by closing stream, which signals the streaming command process to stop
		sLogger.debug("Closing stream to STDIN for child process.");
		mStdinWriter.close();
		
		// block until process ends
		int exitCode = mProcess.waitFor();
		sLogger.debug(String.format("Process done with exit code %s", exitCode));
		
		// wait for thread(s) to finish up
		sLogger.debug("Waiting for STDOUT and STDOUT gobbler threads to complete.");
		mStderrThread.join();
		mStdoutThread.join();
		
		// check if process exited abnormally
		StringBuilder sb = new StringBuilder();
		for (String line: mStderrGobbler.getData()) {
			sb.append(line);
			sb.append(System.getProperty("line.separator"));
		}
		String stderr = sb.toString();
		
		// dump info to bean and return
		ProcessOutput output = new ProcessOutput();
		output.exitCode = exitCode;
		output.stderr = stderr;
		return output;
	}
}
