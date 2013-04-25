/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import com.google.gson.Gson;
import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.history.History;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

/**
 * The FanPipe works as follows.
 * @input - history (a list of strings with metadata)
 * h[1], h[2], ..., h[n]
 * 
 * The fan pipe assumes that h[n] (the last element in the history) is "fan-able".
 * A column can be fan-able if it is a JSON array, or if it is of type JSON and the path has multiple values.
 * (This current implementation only considers JSON arrays.)
 * 
 * The fan pipe extracts the string h[n] into a list a[1], a[2],... a[m].
 * Then, for each element in the list a, it replicates into rows h[1] through h[n-1].
 * 
 * e.g.
 * h[1], h[2], ..., a[1]
 * h[1], h[2], ..., a[2]
 * ...
 * h[1], h[2], ..., a[m]
 * 
 * 
 * @author dquest
 */
public class FanPipe extends AbstractPipe<History,History> {
	private static Logger sLogger = Logger.getLogger(FanPipe.class);
	
    private int index = -1;
    private ArrayList<String> outqueue;

    
    public FanPipe(){
        outqueue = new ArrayList();
    }
    
    public void serializeArrayToOutQueue(ArrayList a){
        for(Object o : a){
            String s = o.toString();
            //sLogger.debug(s);
            outqueue.add(s);
        }
    }
    
    private History history;
    private Gson gson = new Gson();
    
    @Override
    protected History processNextStart() throws NoSuchElementException {
   
        if(outqueue.size() > 0){//if there is stuff in the queue...
        	//then add the element in the queue to the list, copy the history and return it.
        	History clone = (History) history.clone();
        	clone.remove(clone.size()-1);
        	clone.add(outqueue.remove(0));
        	//sLogger.debug("FanPipe: (clone): " + clone);
        	return clone;
        }else {
            history = this.starts.next();
            //sLogger.debug("FanPipe: history: " + history);
			//sLogger.debug("FanPipe (header): " + History.getMetaData().getColumnHeaderRow("\t"));
            String json = history.get(history.size()+index);
            //check that the json string is indeed a json array
            if(!json.startsWith("[")){
                //if it is not, just pass along the history...
                return history;
            }
            //sLogger.debug(json);
            serializeArrayToOutQueue(gson.fromJson(json, ArrayList.class));
            if(outqueue.size() < 1){
                throw new NoSuchElementException();
            }
            return this.processNextStart();
        }
    }

    
}
