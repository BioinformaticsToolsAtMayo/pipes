/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import com.jayway.jsonpath.JsonPath;
import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;

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
    private int jsonpos = 3;
    private int queryResults = 0;
    
    
    /** private variables for getting at the landmark information */
    private JsonPath landmarkPath;
    private JsonPath minBPPath;
    private JsonPath maxBPPath;
    
    public OverlapPipe(String tabixDataFile) throws IOException{
        queryDone = false;
        tr = new TabixReader(tabixDataFile);
        landmarkPath = JsonPath.compile(CoreAttributes._landmark.toString());
        minBPPath = JsonPath.compile(CoreAttributes._minBP.toString());
        maxBPPath = JsonPath.compile(CoreAttributes._maxBP.toString());
    }
    
    @Override
    protected List<String> processNextStart() throws NoSuchElementException {
        //If I have never recieved a history... then get a history from my source
        if(history == null){
            if(this.starts.hasNext()){
                history = this.starts.next();
                queryResults = 0;
            }else {
                throw new NoSuchElementException();
            }
        }else {//I have an active history
            String json = history.get(history.size()-1);
            //are there additional results from the query?
            if(this.hasNextMatch(json)){
                ArrayList<String> al = new ArrayList();
                al.addAll(history);
                al.add(this.getNextMatch(json));
                return al;
            }else { //there are no matches remaining to pull
                //If this history had zero matches, then I should pass along the history with a blank json object...
                if(queryResults == 0){
                    
                }
            
            }
        }
        return null;
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
    public boolean hasNextMatch(String json){
        if(queue.size() > 0){ 
            return true; 
        }
        if(queryDone == false){ 
 //           resultIterator = query(json);
            queryDone = true;
            results = 0; 
        }
	String line;
	try {
            if ((line = resultIterator.next()) != null) {
                queue.add(line);
		return true;
            } else {
                queryDone = false;
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
	}
	return false;
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
    

    
    public TabixReader.Iterator query(String json) throws IOException{
        String landmark;
        Object o;
        o = landmarkPath.read(json);
	if (o != null) {
            landmark = o.toString();
	}else {
            return null;
        }
        String minBP;     
        o = minBPPath.read(json);
	if (o != null) {
            minBP = o.toString();
	}else {
            return null;
        }
        String maxBP;     
        o = minBPPath.read(json);
	if (o != null) {
            maxBP = o.toString();
	}else {
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
