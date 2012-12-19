/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
import edu.mayo.pipes.bioinformatics.vocab.ComparableObjectInterface;
import edu.mayo.pipes.history.ColumnMetaData;
import edu.mayo.pipes.history.History;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *
 * @author m102417
 * This class is a parent pipe to TabixSameVariant, OverlapPipe and 
 * Other pipes that search a tabix file, get multiple results and then need to 'fan out'
 * replicating the lines of the input for each match
 */
public class TabixParentPipe extends AbstractPipe<History, History>{
    protected History history = null;
    protected Pipe search;
    protected int qcount;
    protected boolean isFirst = true;
    protected ComparableObjectInterface comparableObject;
    protected int historyPos = -1; //position in the history to look for the input to the transform (default the last column)
    
    public TabixParentPipe(String tabixDataFile) throws IOException {
        init(tabixDataFile);
    }
    
    protected void init(String tabixDataFile) throws IOException{
        search = new TabixSearchPipe(tabixDataFile);
        comparableObject = new FilterLogic();
    }
    
    protected History copyAppend(History history, String result){
		History clone = (History) history.clone();
		clone.add(result);    	
		return clone;    
    }
    
    protected void setup(){
        //if it is the first call to the pipe... set it up
        if(isFirst){
            isFirst = false;
            //get the history
            history = this.starts.next();
            qcount = 0;
            search.reset();
            search.setStarts(Arrays.asList(history.get(history.size()-1)));
            
            // add column meta data
            List<ColumnMetaData> cols = History.getMetaData().getColumns();
    		ColumnMetaData cmd = new ColumnMetaData(getClass().getSimpleName());
    		cols.add(cmd);
            
        }
        
    }

    /**
     * This valid logic is for filtering out results that match based on one criteria (e.g. position)
     * but fail to match for another reason (e.g. alt and ref allele don't match or IDs don't match)
     * The way this works, is that in the subclass some comparitor object can be declared, and then
     * you set the 
     */
    protected String validResult = "";
    private boolean valid(ComparableObjectInterface fl){
        String result = (String) search.next();
        boolean ret = fl.same(result,history.get(history.size()+historyPos));
        if(ret){
            validResult = result;
        }else {
            validResult = "";
        }
        return ret;
//        return true;
    }
     
    @Override
    protected History processNextStart() throws NoSuchElementException {
        setup();
        //If the search has another result, append the result to the history
        if(search.hasNext()){
            //System.out.println("Next Search Result...");
            if(valid(comparableObject)){
                qcount++;
                return copyAppend(history, validResult);
            }else {//not a valid result, try again...
                return processNextStart();
            }         
        }else {//otherwise, the search did not have any more results, get the next history
            if(qcount == 0){//we did not have any search results, append empty JSON to the history and send it along
                qcount++; //just to get the history to reset on the next call
                return copyAppend(history,"{}");//return empty result
            }else {//we did have at least one result (perhaps empty).. and they are all done
                history = this.starts.next();
                //reset the pipeline for the search query
                search.reset(); 
                search.setStarts(Arrays.asList(history.get(history.size()+historyPos)));
                qcount = 0;
                //and start pulling data again...
                return processNextStart();
            }
            
        }
    }
    
    /**
     * 
     */
    private class FilterLogic implements ComparableObjectInterface {

        @Override
        public boolean same(String a, String b) {
            return true;
        }

    }
    
    
    
    
    //failed attempts to implement this logic
    //    @Override
//    protected List<String> processNextStart() throws NoSuchElementException {
//    	ArrayList<String> al = null;
////        //If I have never received a history... then get a history from my source
////        if(history == null) {
////        	System.out.println("OverlapPipe: history empty..");
////        	if (this.starts.hasNext()) {            
////                history = this.starts.next();
////                queryResults = 0;
////            } else {
////                throw new NoSuchElementException();
////            }        
////        }
////        System.out.println("I have an active history, with size: " + history.size());
////        String json = history.get(history.size()-1);
////        System.out.println("json="+json);
////        try {
////            if(this.hasNextMatch(json)){
////                al = new ArrayList<String>();
////                al.addAll(history);
////                al.add(this.getNextMatch(json));                
////                return al;
////            }else {
////                //can the iterator return me any more results???
////                if (this.starts.hasNext()) { 
////                    //if no, then reset the history to the next thing.
////                    history = this.starts.next();
////                    queryDone = false;
////                    this.queryResults = 0;
////                }else {
////                    throw new NoSuchElementException();
////                }
////
////            }
////
////
////                    //are there additional results from the query?
////    //            if(this.hasNextMatch(json)){
////    //                al = new ArrayList<String>();
////    //                al.addAll(history);
////    //                al.add(this.getNextMatch(json));
////    //                return al;
////    //            } else { //there are no matches remaining to pull
////    //                //If this history had zero matches, then I should pass along the history with a blank json object...
////    //                if(queryResults == 0){
////    //                	al = new ArrayList<String>();
////    //                    al.addAll(history);
////    //                    al.add(null);
////    //                    return al;
////    //                }            
////    //            }
////        } catch (IOException ex) {
////            Logger.getLogger(OverlapPipe.class.getName()).log(Level.SEVERE, null, ex);
////        }
//        
//            
//       //somehow, I have to get the next history from my source if the query is exausted...
//        //if I get a new history, then queryDone = false
//        
//        
//        return al;
//    }
    
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
    


//    public boolean hasNextMatch(String json) throws IOException {
//    	boolean hasMatch=false;
//        //if there is something in the queue, then return true
//        if(queue.size() > 0){ 
//            return true; 
//        }
//        //if I don't have an active iterator, then I need to do a query to activate it
//        if(queryDone == false){ 
//            resultIterator = query(json);
//            String line;
//            if ((line = resultIterator.next()) != null) {
//                this.queryResults++;
//                queue.add(line);
//                return true;
//            }
//            queryDone = true;
//            results = 0; 
//        }else {
//            //The iterator has more elements that we have not seen yet, it could be true
//            String line;
//            if ((line = resultIterator.next()) != null) {
//                this.queryResults++;
//                queue.add(line);
//                return true;
//            }else {
//                return false;
//            }
//            
//        }
//        return false;
////	String line;
////		try {
////            if ((line = resultIterator.next()) != null) {
////                queue.add(line);
////                hasMatch=true;
////            } else {
////                queryDone = false;
////                hasMatch=false;
////            }
////	    } catch (IOException e) {
////	    	e.printStackTrace();
////		}
////		return hasMatch;
//    }	

//    public String getNextMatch(String json) throws IOException {
//    	if(queue.size() < 1){
//            this.hasNextMatch(json);
//        }
//        String result = queue.pollFirst();
//        String[] split = result.split("\t");
//        return split[jsonpos];
//    }
        
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
    
}
