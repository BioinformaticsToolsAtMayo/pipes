package edu.mayo.pipes;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

public class SplitPipe extends AbstractPipe<String, List<String>> {
	
	private String delimiter;
	
	public SplitPipe(String delim){
		delimiter = delim;
	}

	@Override
	protected List<String> processNextStart() throws NoSuchElementException {	
		while(true) {
		       String parseMe = this.starts.next();
               String[] arr = parseMe.split(delimiter);
               
               List<String> list = new ArrayList<String>();
               for (String s: arr) {
            	   list.add(s);
               }
               
               return list;
               
		       //return ImmutableList.copyOf(Splitter.on(delimiter).split(parseMe)); 
		}
	}

}
