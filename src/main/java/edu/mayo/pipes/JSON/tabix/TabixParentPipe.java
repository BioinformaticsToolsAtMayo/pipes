/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.tinkerpop.pipes.AbstractPipe;


import edu.mayo.pipes.JSON.lookup.LookupPipe;
import edu.mayo.pipes.bioinformatics.vocab.ComparableObjectInterface;
import edu.mayo.pipes.exceptions.InvalidPipeInputException;
import edu.mayo.pipes.history.ColumnMetaData;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.util.BiorProperties;
import edu.mayo.pipes.util.metadata.AddMetadataLines;

import org.apache.log4j.Logger;

/**
 *
 * @author m102417
 * This class is a parent pipe to TabixSameVariant, OverlapPipe and 
 * Other pipes that search a tabix file, get multiple results and then need to 'fan out'
 * replicating the lines of the input for each match
 */
public class TabixParentPipe extends AbstractPipe<History, History>{
    protected History history = null;
    protected TabixSearchPipe search;
    protected int qcount;
    protected boolean isFirst = true;
    protected ComparableObjectInterface comparableObject;
    protected int historyPos = -1; //position in the history to look for the input to the transform (default the last column)
    private String biorCatalogPath = "/data5/bsi/catalogs/bior/v1/";
    private String biorCatalog = "BIOR.";
    private String columnvalue;
    private AddMetadataLines addMetadataLines = new AddMetadataLines();
    public TabixParentPipe(String tabixDataFile) throws IOException {
        init(tabixDataFile);
    }
    /*
     * history postion default is -1 (the previous column)
     * you can overide it with an integer as follows:
     * Positions : 1 2 3 4 5 6 0 
     * 0 : the current postion
     * 1 : the first postion
     * 3 : the third position
     * -2 : the second to last position
     */
    public TabixParentPipe(String tabixDataFile, int historyPosition) throws IOException {
        this.historyPos = historyPosition;
        init(tabixDataFile);
    }
    
    protected void init(String tabixDataFile) throws IOException{
        search = new TabixSearchPipe(tabixDataFile);
        String datasourceproperties = tabixDataFile.replace(".tbi", "").replace(".tsv", "").replace(".bgz","") + ".datasource" + ".properties";
        
        File f = new File(datasourceproperties);
        
        if (f.exists()){
        	
          Properties file = new Properties();
         columnvalue= file.getProperty("CatalogShortUniqueName");
        	
        }
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

            //handle the case where the drill column is greater than zero...
            if(historyPos > 0){
                //recalculate it to be negative...
                historyPos = historyPos - history.size() - 1;
            } else if (historyPos == 0){
            	throw	new InvalidPipeInputException("Invalid Column input",this);
            }

   
            qcount = 0;
            search.reset();
            search.setStarts(Arrays.asList(history.get(history.size() + historyPos)));
            
            // add column meta data
            List<ColumnMetaData> cols = History.getMetaData().getColumns();
            ColumnMetaData cmd;
            if (columnvalue != null && !columnvalue.isEmpty()) {
    	 cmd = new ColumnMetaData("BIOR." + getClass().getSimpleName());
            } else {
           cmd = new ColumnMetaData("BIOR." + columnvalue);
            }
            cols.add(cmd);
            history = addMetadataLines.constructMetadataLine(history, cmd.getColumnName());
        }
    }

    /**
     * This valid logic is for filtering out results that match based on one criteria (e.g. position)
     * but fail to match for another reason (e.g. alt and ref allele don't match or IDs don't match)
     * The way this works, is that in the subclass some comparator object can be declared, and then
     * you set the 
     */
    protected String validResult = "";
    private boolean valid(ComparableObjectInterface fl){
        String result = (String) search.next();
        boolean isSame = fl.same(history.get(history.size()+historyPos),result);
        if(isSame){
            validResult = result;
        }else {
            validResult = "";
        }
        return isSame;
    }
     
    @Override
    protected History processNextStart() throws NoSuchElementException {
    	// Setup only on first row
    	setup();
    	
        while(true){
	        //If the search has another result, append the result to the history
	        if(search.hasNext()){
	            if(valid(comparableObject)){
	                qcount++;
	                History newHist = copyAppend(history, validResult);
	                return newHist;
	            }else {//not a valid result, try again...
	            }         
	        }else {//otherwise, the search did not have any more results, get the next history
	            if(qcount == 0){//we did not have any search results, append empty JSON to the history and send it along
	                qcount++; //just to get the history to reset on the next call
	                History newHist = copyAppend(history,"{}"); //return empty result
	                return newHist; 
	            }else {//we did have at least one result (perhaps empty).. and they are all done
	                history = this.starts.next();
	                //reset the pipeline for the search query
	                search.reset();
	                String jsonWithPosInfo = history.get(history.size()+historyPos);
	                search.setStarts(Arrays.asList(jsonWithPosInfo));
	                qcount = 0;
	            }
	        }
        }
    }
    
    private class FilterLogic implements ComparableObjectInterface {

        @Override
        public boolean same(String a, String b) {
            return true;
        }
    }
}
