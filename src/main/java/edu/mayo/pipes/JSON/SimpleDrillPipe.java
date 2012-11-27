/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import com.jayway.jsonpath.JsonPath;
import com.tinkerpop.pipes.AbstractPipe;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *  A SimpleDrillPipe takes as input a JSON String, and applies the 'json-path' drill operators on the 
 * @author m102417
 */
public class SimpleDrillPipe extends AbstractPipe<String, List<String>>{

    /** if keepJSON is true, it will keep the original JSON string and place it at the end of the list */
    private boolean keepJSON = false;
    private String[] drillPaths;
    private ArrayList<JsonPath> compiledPaths;
    public SimpleDrillPipe(boolean keepJSON, String[] paths){
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
    protected List<String> processNextStart() throws NoSuchElementException {
        if(this.starts.hasNext()){
            List<String> out = new ArrayList<String>();
            String json = this.starts.next();
            for(int i=0;i< compiledPaths.size(); i++){
                JsonPath jsonPath = compiledPaths.get(i);
                Object o = jsonPath.read(json);
		if (o != null) {
                    System.out.println(o.toString());
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
