package edu.mayo.pipes.aggregators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

public class AggregatorPipe extends AbstractPipe {
	private int size = 1;
	private ArrayList<Object> objs = null;
	
	public AggregatorPipe(int size){		
		this.size = size;
	}    	
	
	@Override
	protected Collection<Object> processNextStart() throws NoSuchElementException {
		this.objs = new ArrayList<Object>();		
		for(int i=0; i<size; i++) {
			if(this.starts.hasNext()){
				objs.add(this.starts.next());
            } else {
            	break;
            }                     
		}
        //If there is no objects in the return list, then throw exe
        if(this.objs.size() < 1) throw new NoSuchElementException();
		return this.objs;
	}

}
