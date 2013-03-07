package edu.mayo.pipes.util.exec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Intended to gobble up a stream in a separate Thread.
 * 
 * @author duffp
 *
 */
public class StreamGobbler implements Runnable {

	private static final Logger sLogger = Logger.getLogger(StreamGobbler.class);
	
	private InputStream mInStream;
	private List<String> mQueue = new ArrayList<String>();
	
	
	public StreamGobbler(InputStream inStream) {
		mInStream = inStream;
	}
	
	@Override
	public void run() {
		
		if (sLogger.isDebugEnabled())
			sLogger.debug(String.format("%s started.", Thread.currentThread().getName()));
		
		InputStreamReader inStreamReader = new InputStreamReader(mInStream);
		BufferedReader br = new BufferedReader(inStreamReader);
        try {
        	String line = br.readLine();
        	while (line != null) {
        		if (sLogger.isDebugEnabled())
        			sLogger.debug(String.format("%s read line: %s.", Thread.currentThread().getName(), line));
        		
        		mQueue.add(line);
        		
        		line = br.readLine();
        	}
        }
        catch (IOException e) {
            e.printStackTrace();            
        }
        finally {

        	try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

        	try {
				inStreamReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
		if (sLogger.isDebugEnabled())
			sLogger.debug(String.format("%s stopped.", Thread.currentThread().getName()));
	}
	
	public List<String> getData() {
		return mQueue;
	}
}