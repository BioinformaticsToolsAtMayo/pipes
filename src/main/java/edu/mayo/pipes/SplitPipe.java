package edu.mayo.pipes;

import java.util.Collection;
import java.util.NoSuchElementException;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.tinkerpop.pipes.AbstractPipe;

public class SplitPipe extends AbstractPipe<String, String[]> {
	
	private String delimiter = "\t";
	public SplitPipe(String delim){
		delimiter = delim;
	}

	@Override
	protected String[] processNextStart() throws NoSuchElementException {	
		while(true) {
		       String parseMe = this.starts.next();
                       return parseMe.split(delimiter);
		       //return ImmutableList.copyOf(Splitter.on(delimiter).split(parseMe)); 
		}
	}

}
