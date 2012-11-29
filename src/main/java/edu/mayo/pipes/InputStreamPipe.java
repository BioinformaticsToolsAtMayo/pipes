package edu.mayo.pipes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.tinkerpop.pipes.AbstractPipe;

/**
 * Reads lines from an InputStream such as java.lang.System.in (e.g. STDIN).
 */
public class InputStreamPipe extends AbstractPipe<InputStream, String> {

	private static final Logger sLogger = Logger.getLogger(InputStreamPipe.class);

	private BufferedReader mStreamReader = null;
	
	@Override
	protected String processNextStart() throws NoSuchElementException {
		
		if (mStreamReader == null) {
			InputStream inStream = this.starts.next();
			mStreamReader = new BufferedReader(new InputStreamReader(inStream));
		}

		try {

			String nextLine = mStreamReader.readLine();
			
			if (nextLine == null) {
				// reached end of stream
				mStreamReader.close();
				throw new NoSuchElementException();
			}
			
			return nextLine;
			
		} catch (IOException e) {

			sLogger.error(e.getMessage(), e);

			// TODO: how to throw this?
			throw new RuntimeException(e);
		}
	}
}