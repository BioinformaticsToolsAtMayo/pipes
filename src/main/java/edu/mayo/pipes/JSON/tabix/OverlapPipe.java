/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import com.jayway.jsonpath.JsonPath;
import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.JSON.tabix.TabixReader.Iterator;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
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
 * to get back all strings that overlap, it constructs a query with the core attributes.
 */
public class OverlapPipe extends AbstractPipe<List<String>, List<String>> {    
    private TabixReader tr;
    private List<String> history = null;
    private TabixReader.Iterator resultIterator = null;
    private int jsonpos = 3;
    private int queryResults = 0;
        
    /** private variables for getting at the landmark information */
    private JsonPath landmarkPath;
    private JsonPath minBPPath;
    private JsonPath maxBPPath;
    
    public OverlapPipe(String tabixDataFile) throws IOException {
        queryDone = false;
        tr = new TabixReader(tabixDataFile);
        landmarkPath = JsonPath.compile(CoreAttributes._landmark.toString());
        minBPPath = JsonPath.compile(CoreAttributes._minBP.toString());
        maxBPPath = JsonPath.compile(CoreAttributes._maxBP.toString());
    }
    
    @Override
    protected List<String> processNextStart() throws NoSuchElementException {
    	ArrayList<String> al = null;
        //If I have never received a history... then get a history from my source
        if(history == null) {
        	System.out.println("OverlapPipe: history empty..");
        	if (this.starts.hasNext()) {            
                history = this.starts.next();
                queryResults = 0;
            } else {
                throw new NoSuchElementException();
            }        
        }
        //} else { //I have an active history
        	System.out.println("Active history..");
        	System.out.println(history.size());            
        	String json = history.get(history.size()-1);
            //System.out.println("json="+json);

        	String lastrow = history.get(history.size()-1);
            System.out.println("lastrow="+lastrow);
            json = lastrow.substring(lastrow.lastIndexOf("\t"), lastrow.length());
            //System.out.println(lastrow.substring(lastrow.lastIndexOf("\t"), lastrow.length()));

        	//are there additional results from the query?
            if(this.hasNextMatch(json)){
                al = new ArrayList<String>();
                al.addAll(history);
                al.add(this.getNextMatch(json));
                return al;
            } else { //there are no matches remaining to pull
                //If this history had zero matches, then I should pass along the history with a blank json object...
                if(queryResults == 0){
                	al = new ArrayList<String>();
                    al.addAll(history);
                    al.add(null);
                    return al;
                }            
            }
        //}
        
        
        return al;
    }
    
//    @Override
//    protected List<String> processNextStart() throws NoSuchElementException {
//        try {
//            //if there are additional results, then copy the history, add on the next result to the history and pass it along
//            if(resultIterator == null){
//                ArrayList<String> al = new ArrayList();
//                al.addAll(history);
//
//                return al;
//            }
//
//            //if there are no additional results, then get the next history from your source, 
//            //do the query based on the last element in the history,
//            //and add on the next result to the history and pass it along
//            if(this.starts.hasNext() && resultIterator == null){
//                ArrayList<String> al = new ArrayList();
//                history = this.starts.next();
//                al.addAll(history);
//                getNextMatch(history.get(history.size()-1));
//
//                return al;           
//            } else {
//                throw new NoSuchElementException();
//            }
//        }catch (IOException e){
//            //TODO: log the exception
//            throw new NoSuchElementException();
//        }
//    }
    
    LinkedList<String> queue = new LinkedList();
    private boolean queryDone = true;
    private int results = 0;

    public boolean hasNextMatch(String json) {
    	boolean hasMatch=false;
        if(queue.size() > 0){ 
            return true; 
        }
        if(queryDone == false){ 
        	//resultIterator = query(json);
            queryDone = true;
            results = 0; 
        }
		String line;
		try {
            if ((line = resultIterator.next()) != null) {
                queue.add(line);
                hasMatch=true;
            } else {
                queryDone = false;
                hasMatch=false;
            }
	    } catch (IOException e) {
	    	e.printStackTrace();
		}
		return hasMatch;
    }	

    public String getNextMatch(String json) {
    	if(queue.size() < 1){
            this.hasNextMatch(json);
        }
        return queue.pollFirst();
    }
        
//    public boolean hasNextMatch(String json){
//        
//        if(buffer.size() > 0){
//            return true;
//        }else {
//            return false;
//        }
//        
//    }
    
//    public String getNextMatch(String json) throws IOException{
//        String record;
//        
//        if(resultIterator != null){
//            if((record = resultIterator.next()) != null){
//                return record;
//            }else {
//                resultIterator = null;
//                return getNextMatch(json);
//            }
//        }else {
//             resultIterator = query(json);
//             return getNextMatch(json);
//        }
//    }
    
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
	    o = minBPPath.read(json);
		if (o != null) {
			maxBP = o.toString();
		} else {
			return null;
	    }
		
	    //abc123:7000-13000
	    resultIterator = tquery(landmark + ":" + minBP + "-" + maxBP);
	    return resultIterator;
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
