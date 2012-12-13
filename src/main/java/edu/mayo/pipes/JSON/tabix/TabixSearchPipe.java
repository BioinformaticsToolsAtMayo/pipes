/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import com.jayway.jsonpath.JsonPath;
import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.JSON.tabix.TabixReader;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;

import java.io.File;
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
    private int extendminbp = 0;
    private int extendmaxbp = 0;

    
    public TabixSearchPipe(String tabixDataFile) throws IOException{
        init(tabixDataFile);
    }
    
    /**
     * 
     * @param tabixDataFile
     * @param minBPExtend  the amount you would like to extend the boundary 
     * @param maxBPExtend
     * @throws IOException 
     */
    
    public TabixSearchPipe(String tabixDataFile, int minBPExtend, int maxBPExtend) throws IOException{
        init(tabixDataFile);
        this.extendmaxbp = maxBPExtend;
    }
    
    public TabixSearchPipe(String tabixDataFile, int jsonpos) throws IOException{
        init(tabixDataFile);
        this.jsonpos = jsonpos;
    }
    
    private void init(String tabixDataFile) throws IOException {
    	if (new File(tabixDataFile).isFile()){
    		tr = new TabixReader(tabixDataFile);
    	} else {
    		throw new IOException("TabixSearchPipe init(tabixDataFile) requires tabixDataFile to be a valid file. ");
    	}
        landmarkPath = JsonPath.compile(CoreAttributes._landmark.toString());
        minBPPath = JsonPath.compile(CoreAttributes._minBP.toString());
        maxBPPath = JsonPath.compile(CoreAttributes._maxBP.toString());     
    }
    
    private void requery() throws NoSuchElementException, IOException {
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
            if(record != null) {
                return format(record);
            } else {
                records = null;
                query = null;
                requery();
                record = records.next();
                if(record != null){
                    return format(record);
                } else {
                    throw new NoSuchElementException();
                }                
            }
        } catch (Exception ex) {
            //Logger.getLogger(TabixSearchPipe.class.getName()).log(Level.SEVERE, null, ex);
            //System.out.println("TabixSearchPipe.processNextStart() Failed : " + ex.getMessage());            
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
                        if(extendminbp != 0){
                           int t = Integer.parseInt(minBP);
                           minBP = String.valueOf( t - extendminbp );
                        }
		} else {
			return null;
	    }
		
		//_maxBP
	    String maxBP;     
	    o = maxBPPath.read(json);
		if (o != null) {
			maxBP = o.toString();
                        if(extendmaxbp != 0){
                           int t = Integer.parseInt(minBP);
                           minBP = String.valueOf( t - extendmaxbp );
                        }
		} else {
			return null;
	    }
		
	    //abc123:7000-13000
	    records = tquery(landmark + ":" + minBP + "-" + maxBP);
	    
	    return records;
    }
    /**
     * tquery takes a tabix style query, e.g. tabix genes.tsv.bgz 17:10000,20000
     * @param query = 17:10000,20000 
     * @return  landmark:min:max
     * @throws IOException 
     */
    public TabixReader.Iterator tquery(String query) throws IOException {
        //System.out.println("Query to Tabix File: " + query);
        TabixReader.Iterator records = tr.query(query);
        return records;
    }
    
}
