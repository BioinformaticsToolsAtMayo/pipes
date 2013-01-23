package edu.mayo.pipes;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *  MergePipe takes a list of strings, and merges them into a single string.
 * @author m102417 Daniel Quest, Michael Meiners
 */
public class MergePipe extends AbstractPipe<List<String>, String>{
    private String delimiter = "\t";
    private final String EOL = "\n";
    private boolean mIsAddNewline = false;
    public MergePipe(String delim){
        this.delimiter = delim;
    }

    public MergePipe(String delim, boolean appendNewlines){
        this.delimiter = delim;
        mIsAddNewline = appendNewlines;
    }
    
    @Override
    protected String processNextStart() throws NoSuchElementException {
        if( ! this.starts.hasNext() )
        	throw new NoSuchElementException();
        
	    StringBuilder sb = new StringBuilder();
	    List<String> cols = this.starts.next();
	    for(int i=0;i<cols.size();i++){
	    	sb.append(cols.get(i));
	    	if(i < cols.size()-1)
	    		sb.append(delimiter);
	    	else if(mIsAddNewline)
	    		sb.append(EOL);
	    }
	    return sb.toString();
    }
    
}
