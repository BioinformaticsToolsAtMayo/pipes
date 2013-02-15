/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.lookup;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.JSON.lookup.lookupUtils.IndexUtils;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.bioinformatics.vocab.ComparableObjectInterface;
import edu.mayo.pipes.history.ColumnMetaData;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.util.index.FindIndex;
import edu.mayo.pipes.util.index.H2Connection;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author m102417
 */
public class LookupPipe extends AbstractPipe<History,History> {
    private IndexUtils utils = new IndexUtils();
    private Connection dbConn;
    protected History history = null;
    protected int qcount;
    protected boolean isFirst = true;
    protected ComparableObjectInterface comparableObject;
    protected int historyPos = -1; //position in the history to look for the input to the transform (default the last column)
    private File bgzipFile;
    /** the column for the json in the catalog (usually 3 if it is a bed-like-file) */
    private int jsonpos = 3;
    private FindIndex fi = new FindIndex();
    private IndexUtils iutil = null;
    
    /** this holds the indexes we need to get data for */
    private LinkedList<Long> posqueue = new LinkedList<Long>();
    

    /**
     * 
     * @param args
     */
    public static void main(String[] args){
        String dataFile = "src/test/resources/testData/tabix/genes.tsv.bgz";
        String indexFile = "src/test/resources/testData/tabix/index/genes.HGNC.idx.h2.db";
        LookupPipe lookup = new LookupPipe(indexFile, dataFile);
        String s1 = "5";
        String s2 = "7";
        String s3 = "8";
        Pipeline p = new Pipeline(new HistoryInPipe(), 
                                    lookup,
                                    new PrintPipe()
                                );
        p.setStarts(Arrays.asList(s1,s2,s3));
        for(int i=0; p.hasNext(); i++){
            p.next();
        }
        return;
    }
    
    
    /**
     * 
     * @param h2db - the index file we want to use to do the lookup
     * convention is:
     * There is one new folder called ./index at the same level as ./scratch that 
     * contains all the index files for a catalog.  
     * These index files will be named:
     * <catalog_name>.<json_path>.idx.<index_type>
     * E.g. For genes.tsv.bgz we could have genes.HGNC.idx.h2.db
     */
    public LookupPipe(String dbIndexFile, String catalog){
        bgzipFile = new File(catalog);
        //String truncate = dbIndexFile.replace("h2.db", "");
        H2Connection c = new H2Connection(dbIndexFile);
        dbConn = c.getConn();
        iutil = new IndexUtils(bgzipFile);
    }
    
    public List<String> getIDs(List<History> hs, int col){
        List<String> ids = new ArrayList<String>();
        for(int i=0; i<hs.size(); i++){
            History h = hs.get(i);
            ids.add(h.get(col));
        }
        return ids;
    }

    protected void setup(){
        //if it is the first call to the pipe... set it up
        if(isFirst){
            isFirst = false;

            //handle the case where the drill column is greater than zero...
            if(historyPos > 0){
                //recalculate it to be negative...
                historyPos = historyPos - history.size() - 1;
            }

            //get the history
            history = this.starts.next();
            qcount = 0;
            //if we had a dependent pipe, which I don't think we do
            //search.reset();
            //search.setStarts(Arrays.asList(history.get(history.size() + historyPos)));
            
            // add column meta data
            List<ColumnMetaData> cols = History.getMetaData().getColumns();
    		ColumnMetaData cmd = new ColumnMetaData(getClass().getSimpleName());
    		cols.add(cmd);
        }
    }
    
    @Override
    public History processNextStart() throws NoSuchElementException {
    	setup();
    	
        while(true){
	        //If the queue has another result, append the result to the history
	        if(posqueue.size() > 0) {
	        	qcount++;
                Long next = posqueue.poll();
                String line;                       
                String json = "{}";
                try {
                    line = iutil.getZipLinesByPostion(next);
                    if(line.length() > 2){//have to have {} at the least
                        String[] split = line.split("\t");
                        json = split[jsonpos];
                    }
                } catch (Exception ex) {
                    Logger.getLogger(LookupPipe.class.getName()).log(Level.SEVERE, null, ex);
                }
                return copyAppend(history, json);   
	        } else {//otherwise, the search did not have any more results, get the next history
	            if(qcount == 0){//we did not have any search results, append empty JSON to the history and send it along
	                qcount++; //just to get the history to reset on the next call
	                return copyAppend(history,"{}");//return empty result
	            } else {//we did have at least one result (perhaps empty).. and they are all done
	                history = this.starts.next();
	                //reset the pipeline for the search query
	                posqueue = new LinkedList<Long>();
                        //get the ID we need to search with out of it...
                        String id = history.get(history.size() + historyPos);
                        try {
                            //query the index...
                        	LinkedList<Long> find = fi.find(id, dbConn);
                        	
                            //build the posqueue                            
                            posqueue = (LinkedList<Long>) find;
                            
                        } catch (SQLException ex) {
                            Logger.getLogger(LookupPipe.class.getName()).log(Level.SEVERE, null, ex);
                        }
	                
	                qcount = 0;
	            }
	        }
        }
    }
    
    protected History copyAppend(History history, String result){
		History clone = (History) history.clone();
		clone.add(result);    	
		return clone;    
    }
    
    
    public int getJsonpos() {
        return jsonpos;
    }

    public void setJsonpos(int jsonpos) {
        this.jsonpos = jsonpos;
    }
    
}
