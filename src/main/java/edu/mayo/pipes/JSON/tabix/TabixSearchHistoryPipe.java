/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import java.io.IOException;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.history.History;

/**
 * @author Michael Meiners
 * Takes a History object (with columns), and appends on additional columns that come from the tabix indexed catalog
 */
public class TabixSearchHistoryPipe extends AbstractPipe<History, History> {
    private TabixSearchPipe mTabixSearchPipe;
    private History history = null;
    private TabixReader.Iterator tabixRecords = null;
    
    public TabixSearchHistoryPipe(String tabixDataFile) throws IOException{
    	this(tabixDataFile, 0, 0);
    }
    
    /**
     * @param tabixDataFile
     * @param minBPExtend  the amount you would like to extend the minBP boundary to the left
     * @param maxBPExtend  the amount you would like to extend the maxBP boundary to the right
     * @throws IOException 
     */
    public TabixSearchHistoryPipe(String tabixDataFile, int minBPExtend, int maxBPExtend) throws IOException{
    	mTabixSearchPipe = new TabixSearchPipe(tabixDataFile, minBPExtend, maxBPExtend);
    }
    
    @Override
    protected History processNextStart() throws NoSuchElementException {
        try {
            String tabixRecord = null;
            requery();
            
            //Get the next Tabix search result
            tabixRecord = tabixRecords.next();
            if(tabixRecord != null) {
                return addTabixResultsColumnsToHistory(history, tabixRecord);
            } else {
            	tabixRecords = null;
            	history = null;
                requery();
                tabixRecord = tabixRecords.next();
                if(tabixRecord != null){
                	return addTabixResultsColumnsToHistory(history, tabixRecord);
                } else {
                    throw new NoSuchElementException();
                }                
            }
        } catch (Exception ex) {
        	ex.printStackTrace();
        }
    	throw new NoSuchElementException();
    }
    
    private void requery() throws NoSuchElementException, IOException {
        if(tabixRecords == null){
            if(history == null){
                if(this.starts.hasNext()){
                	// Get the next history row that we will use to query tabix
                	history = this.starts.next();
                }else {
                    throw new NoSuchElementException();
                }
            }
            // Get JSON from history - assume it is the last column
            String json = history.get(history.size()-1);
            tabixRecords = mTabixSearchPipe.query(json);
        }
    }
    
    private History addTabixResultsColumnsToHistory(History history, String tabixRecord) {
    	// Split the tabix record by tabs
    	String[] tabixColumns = tabixRecord.split("\t");
    	for(String col : tabixColumns) {
    		history.add(col);
    	}
    	return history;
    }

}
