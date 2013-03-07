package edu.mayo.pipes.util.exec;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class StreamingCommandProcess extends BaseCommandProcess {

	private static final Logger sLogger = Logger.getLogger(StreamingCommandProcess.class);
	
	private Process mProcess;
	
	private Thread mStderrThread;	
	private ByteArrayOutputStream mStderrOutputStream = new ByteArrayOutputStream();
	
	// used to write data to STDIN of the child process
	private PrintWriter mStdinWriter;
	
	// used to read data from STDOUT of the child process
	private BufferedReader mStdoutReader;
	
	// default buffer is 1KB
	public static final int DEFAULT_BUFFER_SIZE = 1024;
	
	private int mStreamBufferSize;
		
	public StreamingCommandProcess(
			String command,
			String[] commandArgs,
			Map<String, String> customEnv,
			boolean useParentEnv) {		
		this(command, commandArgs, customEnv, useParentEnv, DEFAULT_BUFFER_SIZE);
	}

	public StreamingCommandProcess(
			String command,
			String[] commandArgs,
			Map<String, String> customEnv,
			boolean useParentEnv,			
			int streamBufferSize) {

		super(command, commandArgs, customEnv, useParentEnv);
		
		mStreamBufferSize = streamBufferSize;
	}
	
	public void start() throws IOException {
		// start process
		mProcess = Runtime.getRuntime().exec(mCmdArray, mEnvironment);
		
		// construct a Writer that allows us to write to STDIN of the child process
		mStdinWriter = new PrintWriter(new OutputStreamWriter(mProcess.getOutputStream()));
		
		// construct a Reader that allows us to read from STDOUT of the child process
		mStdoutReader = new BufferedReader(new InputStreamReader(mProcess.getInputStream()));
		
		// connect STDERR from script process and store in local memory
		// STDERR [script process] ---> local byte array
		StreamConnector stderrConnector = new StreamConnector(mProcess.getErrorStream(), mStderrOutputStream, mStreamBufferSize);
		mStderrThread = new Thread(stderrConnector);
		mStderrThread.start();		
	}

	public void send(List<String> dataLines){
		for (String line: dataLines) {
			mStdinWriter.println(line);
		}
		mStdinWriter.flush();
	}
	
	public List<String> receive() throws IOException {

		List<String> dataLines = new ArrayList<String>();
		String line = mStdoutReader.readLine();
		while (line != null) {
			dataLines.add(line);

			if (mStdoutReader.ready()) {
				// grab next line if there is one
				line = mStdoutReader.readLine();
			} else {
				// no more data to grab
				line = null;
			}
		}
		
		return dataLines;
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
		mStdinWriter.close();
		
		// block until process ends
		int exitCode = mProcess.waitFor();
		
		// wait for thread(s) to finish up
		mStderrThread.join();
		
		// check if process exited abnormally
		String stderr = mStderrOutputStream.toString("UTF-8");
		
		// dump info to bean and return
		ProcessOutput output = new ProcessOutput();
		output.exitCode = exitCode;
		output.stderr = stderr;
		return output;
	}
}
