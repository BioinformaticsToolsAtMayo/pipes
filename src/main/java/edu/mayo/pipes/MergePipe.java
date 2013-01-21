/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *  MergePipe takes a list of strings, and merges them into a single string.
 * @author m102417
 */
public class MergePipe extends AbstractPipe<List<String>, String>{
    private String delimiter = "\t";
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
	    if(cols.size()==1){
	    	return cols.get(0) + (mIsAddNewline ? "\n" : "");
	    }
	    for(int i=0;i<cols.size()-1;i++){
	    	String s = cols.get(i);
	    	sb.append(s);
	    	sb.append(delimiter);
	    }
	    sb.append(cols.get(cols.size()-1));
	    if(mIsAddNewline){
	    	sb.append("\n");
	    }
	    return sb.toString();
    }
    
}
