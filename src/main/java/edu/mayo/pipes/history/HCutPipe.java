/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.history;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * H cut works just like cut only on history objects (removing the meta data at the same time)
 * note cut is 1-based not zero based!
 * @author m102417
 */
public class HCutPipe extends AbstractPipe<History, History> {
    private ArrayList<Integer> cols;
    private boolean muck = true; // do we need to muck with the header?
    /**
     * 
     * @param columns - list of numbers for the columns we want to cut.  Negative values are allowed.
     */
    public HCutPipe(int[] columns){
        init(columns);
        muck = true;
    }
    
    /**
     * 
     * @param muck - set muck to false if you don't want it to muck with the headers
     * this is commonly used in publishers.
     */
    public HCutPipe(boolean muck, int[] columns){
        init(columns);
        this.muck = muck;
    }
    
    private void init(int[] columns){
        cols = new ArrayList();
        Arrays.sort(columns);
        for(int i=columns.length-1; i>=0; i--){
            //System.out.println(columns[i]);
            cols.add(columns[i]);
        }
    }
    
    
    @Override
    public void reset() {
        muck = true;
        super.reset();
    }
    
    @Override
    protected History processNextStart() throws NoSuchElementException {
        History h = this.starts.next();
        List<ColumnMetaData> cmd = History.getMetaData().getColumns();
        
        for(int i=0; i<cols.size(); i++){
        	int cutCol = cols.get(i);
	        //handle the case where the drill column is greater than zero...
	        if(cutCol > 0){
	            //recalculate it to be negative...
	            cutCol = cutCol - h.size() - 1;
	        }
	        if(h.size() == 1){
	            cutCol = -1;
	        }        
        	
        	int m = h.size() + cutCol;

        	h.remove(m);
	        if(muck){
	        	cmd.remove(m);
	        }
        }
        muck = false;
        return h;
    }
    
}
