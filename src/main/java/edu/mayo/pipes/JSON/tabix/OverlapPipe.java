/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import com.jayway.jsonpath.JsonPath;
import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.bioinformatics.CoreAttributes;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *
 * @author dquest
 * Overlap takes a list of strings in.  The last string in the list is a JSON string.  
 * It then drills into the JSON String, to get the core attributes it needs:
 * mainly: 
	_landmark,
	_minBP,
	_maxBP,
 * to get back all strings that overlap, it constructs a query with the core attributes
 */
public class OverlapPipe extends AbstractPipe<List<String>, List<String>>{
    
    private TabixReader tr;
    private List<String> history = null;
    private TabixReader.Iterator resultIterator = null;
    
    
    /** private variables for getting at the landmark information */
    private JsonPath landmarkPath;
    private JsonPath minBPPath;
    private JsonPath maxBPPath;
    
    public OverlapPipe(String tabixDataFile) throws IOException{
        tr = new TabixReader(tabixDataFile);
        landmarkPath = JsonPath.compile(CoreAttributes._landmark.toString());
        minBPPath = JsonPath.compile(CoreAttributes._minBP.toString());
        maxBPPath = JsonPath.compile(CoreAttributes._maxBP.toString());
    }
    
    @Override
    protected List<String> processNextStart() throws NoSuchElementException {
        if(this.starts.hasNext() && resultIterator == null){
            history = this.starts.next();
            return history;
        //}else if(resultIterator != null) {
            
        } else {
            throw new NoSuchElementException();
        }
    }
    

    
    public TabixReader.Iterator query(String json){
        TabixReader.Iterator records = tr.query(0, 1000,14000);
        return records;
    }
    
//    public TabixReader.Iterator query() throws IOException{
//        String record = null;
//        TabixReader.Iterator records = tr.query(0, 1000,14000);
//        
//        while((record = records.next()) != null){
//            System.out.println(record);
//        }
//    }
    
    public TabixReader.Iterator tquery(String query) throws IOException{
        TabixReader.Iterator records = tr.query(query);
        return records;
    }
    
    
    
}
