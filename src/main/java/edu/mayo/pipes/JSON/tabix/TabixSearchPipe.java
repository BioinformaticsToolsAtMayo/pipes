/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import com.jayway.jsonpath.JsonPath;
import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.exceptions.InvalidPipeInputException;

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
        this.extendminbp = minBPExtend;
    }
    
    public TabixSearchPipe(String tabixDataFile, int jsonpos) throws IOException{
        init(tabixDataFile);
        this.jsonpos = jsonpos;
    }
    
    private void init(String tabixDataFile) throws IOException {
    	if (new File(tabixDataFile).isFile()){
    		tr = new TabixReader(tabixDataFile);
    	} else {
    		System.err.println("File is not valid: " + tabixDataFile);
    		throw new IOException("TabixSearchPipe init(tabixDataFile) requires tabixDataFile to be a valid file. \n"
    				+ "File: " + tabixDataFile);
    	}
        landmarkPath = JsonPath.compile(CoreAttributes._landmark.toString());
        minBPPath = JsonPath.compile(CoreAttributes._minBP.toString());
        maxBPPath = JsonPath.compile(CoreAttributes._maxBP.toString());     
    }
    
    public String format(String s) throws InvalidPipeInputException{
        String[] split = s.split("\t");
        return split[jsonpos];
     
    }
    
    String snpJsonIn = null;
    TabixReader.Iterator records = null;
    @Override
    protected String processNextStart() throws NoSuchElementException {
        try {
            String record = null;
            requery();
            
            if( records == null ) {
            	// no results from TABIX search            	
            	// reset "snpJsonIn" to NULL so that requery() method will pull the next JSON
            	snpJsonIn = null;
            	
            	throw new NoSuchElementException("There were no tabix search results to match the variant, or chromosome not found in tabix index.");
            }
            
            //give you back the next query result
            record = records.next();
            if(record != null) {
                return format(record);
            } else {
                records = null;
                snpJsonIn = null;
                requery();
                record = records.next();
                if(record != null){
                    return format(record);
                } else {
                    throw new NoSuchElementException();
                }
            }
        }catch (Exception e){
            records = null;
            // We may have hit an ArrayIndexOutOfBoundsException from TabixReader.query()  
            // NO results, so set snpJsonIn=null
            // NOTE: This will occur if the variant passed in has a position of 0 - which should not be valid input!!!
            //       If that is the case, make sure to set snpJsonIn=null or it will return no results for ALL subsequent variants
            snpJsonIn = null;
            throw new NoSuchElementException();
        }
    	
    }
    
    public TabixReader.Iterator query(String json) throws IOException {    
    	Object o;

        if(json.equalsIgnoreCase("{}")){
            return null;
        }
        //_landmark
        String landmark;
        o = landmarkPath.read(json);
		if (o != null) {
			landmark = o.toString();
		} else {
	        return null; //never going to get null
	    }
	    
		//_minBP
		String minBP;     
	    o = minBPPath.read(json);
		if (o != null) {
			minBP = o.toString();
			if(extendminbp != 0){
				int t = Integer.parseInt(minBP);
				minBP = String.valueOf( t - extendminbp );
				if(t-this.extendminbp < 0){
					minBP = "0";
				}
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
				int t = Integer.parseInt(maxBP);
				maxBP = String.valueOf( t + extendmaxbp );
			}
		} else {
			return null;
	    }
		
	    //abc123:7000-13000
	    records = tquery(landmark + ":" + minBP + "-" + maxBP);
	    
	    return records;
    }
    
    private void requery() throws NoSuchElementException, IOException {
        if(records == null){
            if(snpJsonIn == null){
                if(this.starts.hasNext()){
                    snpJsonIn = this.starts.next();//get the next json string
                }else {
                    throw new NoSuchElementException();
                }
            }
            records = query(snpJsonIn);
        }
    }
    

    
    /**
     * tquery takes a tabix style query, e.g. tabix genes.tsv.bgz 17:10000,20000
     * @param chrMinMaxQuery = 17:10000,20000 
     * @return  landmark:min:max
     * @throws IOException 
     */
    public TabixReader.Iterator tquery(String chrMinMaxQuery) throws IOException {
        TabixReader.Iterator records = tr.query(chrMinMaxQuery);
        return records;
    }
    
}
