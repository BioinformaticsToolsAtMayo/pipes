package edu.mayo.pipes;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.history.History;

/**
 * Split a string into a History object.
 * This is very similar to SplitPipe, but yields a History object instead of a list of strings
 * @author Michael Meiners (m054457)
 * Date created: Apr 22, 2013
 */
public class String2HistoryPipe extends AbstractPipe<String, List<String>> {
	
	private String delimiter;
	
	public String2HistoryPipe(String delim){
		delimiter = delim;
	}

	@Override
	protected History processNextStart() throws NoSuchElementException {	
		String parseMe = this.starts.next();
		String[] arr = parseMe.split(delimiter);
               
		History hist = new History();
		for (String s: arr) {
			hist.add(s);
		}

		return hist;
	}


}
