/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.exceptions.InvalidJSONException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *
 * @author m102417
 */
public class DrillPipe extends AbstractPipe<List<String>, List<String>>{

    /** if keepJSON is true, it will keep the original JSON string and place it at the end of the list */
    private boolean keepJSON = false;
    private String[] drillPaths;
    private ArrayList<JsonPath> compiledPaths;
    public DrillPipe(boolean keepJSON, String[] paths){
        this.keepJSON = keepJSON;
        this.drillPaths = paths;
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
    protected List<String> processNextStart() throws NoSuchElementException, InvalidJSONException {
        if(this.starts.hasNext()){
            List<String> out = this.starts.next();
            String json = out.remove(out.size()-1);
            //System.out.println("About to Drill: " + json);
            for(int i=0;i< compiledPaths.size(); i++){
                if(!json.startsWith("{")){ //TODO: we may need a more rigourus check to see if it is json.
                    //out.add("."); 
                    throw new InvalidJSONException("A column input to Drill that should be JSON was not JSON, I can't Drill non-JSON columns");
                }else {
                    try {
                        JsonPath jsonPath = compiledPaths.get(i);
                        Object o = jsonPath.read(json);
                        if (o != null) {
                            //System.out.println(o.toString());
                            out.add(o.toString());
                        }
                    }catch(InvalidPathException e){
                        //In general I don't know if we should output an error to the logs, or just output a failed drill e.g. '.'.  
                        //I think failed drill is perhaps better, because what are they going to do with the error?  I think just get angry.
                        //System.out.println("Drill path did not exist for: " + this.drillPaths[i] + " This is the JSON I tried to drill: " + json);
                        out.add(".");
                    }
                }
            }
            if(keepJSON){
                out.add(json);
            }
            
            return out;
        }else{
            throw new NoSuchElementException();
        }
    }
    
}

