/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.exceptions.InvalidJSONException;
import edu.mayo.pipes.history.ColumnMetaData;
import edu.mayo.pipes.history.History;

/**
 *
 * @author m102417
 */
public class DrillPipe extends AbstractPipe<History, History>{

	private static Logger sLogger = Logger.getLogger(DrillPipe.class);
	
    /** if keepJSON is true, it will keep the original JSON string and place it at the end of the list */
    private boolean keepJSON = false;
    private String[] drillPaths;
    private ArrayList<JsonPath> compiledPaths;
    private boolean addColumnMetaData = true;
    private int drillColumn = -1; //negative value... how many columns to go back (default -1).
    
    public DrillPipe(boolean keepJSON, String[] paths){
        this.keepJSON = keepJSON;
        this.drillPaths = paths;
        setupPaths();
    }
    
    public DrillPipe(boolean keepJSON, String[] paths, int drillColumn){
        this.keepJSON = keepJSON;
        this.drillPaths = paths;
        this.drillColumn = drillColumn;
        setupPaths();
    }
    
    private void setupPaths(){
        compiledPaths = new ArrayList<JsonPath>();
        for(int i = 0; i< drillPaths.length; i++){
            JsonPath jsonPath = JsonPath.compile(drillPaths[i]);
            compiledPaths.add(jsonPath);
        }
        return;
    }
    
    @Override
    protected History processNextStart() throws NoSuchElementException, InvalidJSONException {
        if(this.starts.hasNext()){
            History history = this.starts.next();
            
            //sLogger.debug("DrillPipe: (before): " + history);
            //String headerBefore = History.getMetaData().getColumnHeaderRow("\t");
			//sLogger.debug("DrillPipe: (header-before): " + headerBefore);
			//sLogger.debug("DrillPipe: isKeepJson: " + keepJSON);
			
            //handle the case where the drill column is greater than zero...
            if(drillColumn > 0){
                int size = history.size();
                //recalculate it to be negative...
                drillColumn = drillColumn - history.size() - 1;
            }
            if(history.size() == 1){
                drillColumn = -1;
            }
                        
            if (addColumnMetaData) {
            	List<ColumnMetaData> cols = History.getMetaData().getColumns();
                ColumnMetaData lastJsonCol = cols.remove(cols.size() + drillColumn);

                for (String drillPath: drillPaths) {
            		ColumnMetaData cmd = new ColumnMetaData(drillPath);
            		cols.add(cmd);
            	}
                
                if (keepJSON) {
                	cols.add(lastJsonCol);
                }
                
            	addColumnMetaData = false;
            }
            
            String json = history.remove(history.size() + drillColumn);
            //System.history.println("Abhistory to Drill: " + json);
            for(int i=0;i< compiledPaths.size(); i++){
                if(!json.startsWith("{")){ //TODO: we may need a more rigorous check to see if it is json.
                    //history.add("."); 
                    int reportColumn = history.size() + drillColumn;
                    throw new InvalidJSONException("A column input to Drill that should be JSON was not JSON, I can't Drill non-JSON columns: " + reportColumn + " : " + json);
                }else {
                    try {
                        JsonPath jsonPath = compiledPaths.get(i);
                        Object o = jsonPath.read(json);
                        if (o != null) {
                            //System.history.println(o.toString());
                            history.add(o.toString());
                        }
                    }catch(InvalidPathException e){
                        //In general I don't know if we should historyput an error to the logs, or just historyput a failed drill e.g. '.'.  
                        //I think failed drill is perhaps better, because what are they going to do with the error?  I think just get angry.
                        //System.history.println("Drill path did not exist for: " + this.drillPaths[i] + " This is the JSON I tried to drill: " + json);
                        history.add(".");
                    }
                }
            }
            
            // If keeping the json column, then add the json back in at the end
            if(keepJSON){
                history.add(json);
            }
            // Else, remove the head
            
            //sLogger.debug("DrillPipe: (after): " + history);
            //String headerAfter = History.getMetaData().getColumnHeaderRow("\t");
			//sLogger.debug("DrillPipe: (header-after): " + headerAfter);
            return history;
        }else{
            throw new NoSuchElementException();
        }
    }
    
}

