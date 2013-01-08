/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.history;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * H cut works just like cut only on history objects (removing the meta data at the same time)
 * @author m102417
 */
public class HCutPipe extends AbstractPipe<History, History> {
    private int[] cols;
    /**
     * 
     * @param columns - list of 
     */
    public HCutPipe(int[] columns){
        cols = new int[columns.length];
        Arrays.sort(columns);
        cols = columns;
        
    }
    @Override
    protected History processNextStart() throws NoSuchElementException {
        History h = this.starts.next();
        List<ColumnMetaData> cmd = History.getMetaData().getColumns();
                
        for(int i=0; i<cols.length; i++){
          h.remove(cols[i]);
          cmd.remove(cols[i]);
        }
        return null;
    }
    
}
