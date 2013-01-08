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
     * @param columns - list of 
     */
    public HCutPipe(int[] columns){
        cols = new ArrayList();
        Arrays.sort(columns);
        for(int i=columns.length-1; i>=0; i--){
            //System.out.println(columns[i]);
            cols.add(columns[i]);
        }
        muck = true;
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
                
        //System.out.println(cmd.size());
        for(int i=0; i<cols.size(); i++){
          int m = cols.get(i)-1;
          h.remove(m);
          if(muck){
            cmd.remove(m);
          }
        }
        muck = false;
        return h;
    }
    
}
