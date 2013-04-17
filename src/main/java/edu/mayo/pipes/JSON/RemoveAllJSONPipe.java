/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.history.ColumnMetaData;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryMetaData;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *
 * @author m102417
 */
public class RemoveAllJSONPipe extends AbstractPipe<History,History>{

    boolean first = true;
    @Override
    protected History processNextStart() throws NoSuchElementException {
        History h = this.starts.next();
        //go through the columns, removing any JSON
        for(int i=0; i<h.size();i++){
            if(h.get(i).startsWith("{")){//if it is JSON, remove it.
                h.remove(i);
                if(first){
                    HistoryMetaData metaData = History.getMetaData();
                    List<ColumnMetaData> columns = metaData.getColumns();
                    columns.remove(i);
                }
            }
        }
        first = false;
        return h;
    }
    
}
