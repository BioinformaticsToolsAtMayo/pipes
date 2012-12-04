/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import com.jayway.jsonpath.JsonPath;
import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.JSON.tabix.TabixReader;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author m102417
 * takes in a JSON string and gives back JSON matches, if not found, then return {}.
 * 
 * This 'pipe' is only really a pipe out of convenience, it is not robustly tested to put it in 
 * any pipeline, it is intended to be used by OverlapPipe and other JSON/Tabix oriented pipes to reduce 
 * implementation complexity.
 * 
 */
public class TabixSearchPipe extends AbstractPipe<String, String>{
    private TabixReader tr;
    private int jsonpos = 3;
        
    /** private variables for getting at the landmark information */
    private JsonPath landmarkPath;
    private JsonPath minBPPath;
    private JsonPath maxBPPath;

    
    public TabixSearchPipe(String tabixDataFile) throws IOException{
        init(tabixDataFile);
    }
    
    public TabixSearchPipe(String tabixDataFile, int jsonpos) throws IOException{
        init(tabixDataFile);
        this.jsonpos = jsonpos;
    }
    
    private void init(String tabixDataFile) throws IOException{
        tr = new TabixReader(tabixDataFile);
        landmarkPath = JsonPath.compile(CoreAttributes._landmark.toString());
        minBPPath = JsonPath.compile(CoreAttributes._minBP.toString());
        maxBPPath = JsonPath.compile(CoreAttributes._maxBP.toString());     
    }
    
    private void requery() throws IOException, NoSuchElementException{
            if(records == null){
                if(query == null){
                    if(this.starts.hasNext()){
                        query = this.starts.next();//get the next json string
                    }else {
                        throw new NoSuchElementException();
                    }
                }
                records = query(query);
            }
    }
    
    public String format(String s){
        String[] split = s.split("\t");
        return split[jsonpos];
    }
    
    String query = null;
    TabixReader.Iterator records = null;
    @Override
    protected String processNextStart() throws NoSuchElementException {
        try {
            String record = null;
            requery();
            
            record = records.next();//give you back the next query result
            if(record != null){
                return format(record);
            }else {
                records = null;
                query = null;
                requery();
                record = records.next();
                if(record != null){
                    return format(record);
                }else {
                    throw new NoSuchElementException();
                }
                
            }
                    //return record; //split the record or something!
            
            
    //        if(query == null){
    //            if(this.starts.hasNext()){
    //                query = this.starts.next();
    //                try {
    //                    records = query(query);
    //                } catch (IOException ex) {
    //                    Logger.getLogger(TabixSearchPipe.class.getName()).log(Level.SEVERE, null, ex);
    //                    throw new NoSuchElementException();
    //                }
    //            }else {
    //                throw new NoSuchElementException();
    //            }          
    //        }
    //        try {
    //            String record;
    //            while((record = records.next()) != null){
    //                return record; //split the record or something!
    //            }  
    //            throw new NoSuchElementException();
    //        } catch(IOException e){
    //        }
    //        }
        } catch (IOException ex) {
            Logger.getLogger(TabixSearchPipe.class.getName()).log(Level.SEVERE, null, ex);
        }
        throw new NoSuchElementException();
    }
    
    public TabixReader.Iterator query(String json) throws IOException {    
        Object o;
        
        //_landmark
        String landmark;
        o = landmarkPath.read(json);
		if (o != null) {
			landmark = o.toString();
		} else {
	        return null;
	    }
	    
		//_minBP
		String minBP;     
	    o = minBPPath.read(json);
		if (o != null) {
			minBP = o.toString();
		} else {
			return null;
	    }
		
		//_maxBP
	    String maxBP;     
	    o = maxBPPath.read(json);
		if (o != null) {
			maxBP = o.toString();
		} else {
			return null;
	    }
		
	    //abc123:7000-13000
	    records = tquery(landmark + ":" + minBP + "-" + maxBP);
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
        System.out.println("Query to Tabix File: " + query);
        TabixReader.Iterator records = tr.query(query);
        return records;
    }
    
}
