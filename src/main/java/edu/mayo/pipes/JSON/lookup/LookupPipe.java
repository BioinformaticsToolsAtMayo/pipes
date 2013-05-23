/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.lookup;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.JSON.lookup.lookupUtils.IndexUtils;
import edu.mayo.pipes.bioinformatics.vocab.ComparableObjectInterface;
import edu.mayo.pipes.history.ColumnMetaData;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.util.index.FindIndex;
import edu.mayo.pipes.util.index.H2Connection;

/**
 *
 * @author m102417
 */
public class LookupPipe extends AbstractPipe<History,History> {
    private IndexUtils mUtils = new IndexUtils();
    private Connection mDbConn;
    protected History mHistory = null;
    protected int mQcount;
    protected boolean mIsFirst = true;
    protected ComparableObjectInterface mComparableObject;
    protected int mHistoryPos = -1; //position in the history to look for the input to the transform (default the last column)
    private File mBgzipFile;
    /** the column for the json in the catalog (usually 3 if it is a bed-like-file) */
    private int mJsonpos = 3;
    private FindIndex mFindIndex;
    private boolean mIsKeyAnInteger = false;
    /** this holds the indexes we need to get data for */
    private LinkedList<Long> mPosqueue = new LinkedList<Long>();
    private int drillColumn = -1; //negative value... how many columns to go back (default -1).
     
    /**
     * @param catalogFile - catalog to lookup the key in to find the row
     * @param indexFile - the index file we want to use to do the lookup
     *   convention is:
     *   There is one new folder called ./index at the same level as ./scratch that 
     *   contains all the index files for a catalog.  
     *   These index files will be named:
     *   <catalog_name>.<json_path>.idx.<index_type>
     *   E.g. For genes.tsv.bgz we could have:
     *   	genes.HGNC.idx.h2.db	OR
     *   	genes.HGNC.idx.txt.gz
     * @throws SQLException 
     */
    public LookupPipe(String catalogFile, String indexFile) {
    	this(catalogFile, indexFile, -1, false);
    }   
    
    /**
     * 
     * @param catalogFile - actual catalog file
     * @param indexFile - h2 index file path
     * @param drillColumn - column number
     */
    public LookupPipe(String catalogFile, String indexFile, int drillColumn) {
    	this(catalogFile, indexFile, drillColumn, false);
    }       

    /**
     * 
     * @param catalogFile - actual catalog file
     * @param indexFile - h2 index file path
     * @param drillColumn - column number
     */
    public LookupPipe(String catalogFile, String indexFile, int drillColumn, boolean isKeyCaseSensitive) {
        mBgzipFile = new File(catalogFile);
        //String truncate = dbIndexFile.replace("h2.db", "");
        H2Connection h2DbConn = new H2Connection(indexFile);
        mDbConn = h2DbConn.getConn();
        mUtils = new IndexUtils(mBgzipFile);
        mIsKeyAnInteger = IndexUtils.isKeyAnInteger(mDbConn);
        mFindIndex = new FindIndex(mDbConn, isKeyCaseSensitive);        
        this.drillColumn = drillColumn;
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
        if(mIsFirst){
            mIsFirst = false;

            //handle the case where the drill column is greater than zero...
            /*if(mHistoryPos > 0){
                //recalculate it to be negative...
                mHistoryPos = mHistoryPos - mHistory.size() - 1;
            }*/
 
            //get the history
            mHistory = this.starts.next();
            
            if(drillColumn > 0){
                int size = mHistory.size();
                //recalculate it to be negative...
                drillColumn = drillColumn - mHistory.size() - 1;
            }
            
            if(mHistory.size() == 1){
                drillColumn = -1;
            }
            
            mQcount = 0;
            //now we have to put the stuff in the queue...
            //String id = mHistory.get(mHistory.size() + mHistoryPos);
            String id = mHistory.get(mHistory.size() + drillColumn);
            if (validateIdToFind(id)) {
	            try {
	                    //query the index and build the posqueue
	                    mPosqueue = (LinkedList<Long>)mFindIndex.find(id);
	            } catch (SQLException ex) {
	                    Logger.getLogger(LookupPipe.class.getName()).log(Level.ERROR, null, ex);
	            }
            }
            
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
	        if(mPosqueue.size() > 0) {
	        	mQcount++;
                Long next = mPosqueue.poll();
                String line;                       
                String json = "{}";
                try {
                    line = mUtils.getBgzipLineByPosition(next);
                    if(line.length() > 2){//have to have {} at the least
                        String[] split = line.split("\t");
                        json = split[mJsonpos];
                    }
                } catch (Exception ex) {
                    Logger.getLogger(LookupPipe.class.getName()).log(Level.ERROR, null, ex);
                }
                return copyAppend(mHistory, json);   
	        } else {//otherwise, the search did not have any more results, get the next history
	            if(mQcount == 0){//we did not have any search results, append empty JSON to the history and send it along
	                mQcount++; //just to get the history to reset on the next call
	                return copyAppend(mHistory,"{}");//return empty result
	            } else {//we did have at least one result (perhaps empty).. and they are all done
	                mHistory = this.starts.next();
	                //reset the pipeline for the search query
	                mPosqueue = new LinkedList<Long>();
	                //From history, get the ID we need to search for...
	                
	                //String id = mHistory.get(mHistory.size() + mHistoryPos);
	                String id = mHistory.get(mHistory.size() + drillColumn);
	                if (validateIdToFind(id)) {
		                try {
		                	//query the index and build the posqueue
		                	mPosqueue = (LinkedList<Long>)mFindIndex.find(id);
	 	                } catch (SQLException ex) {
		                	Logger.getLogger(LookupPipe.class.getName()).log(Level.ERROR, null, ex);
		                }
	                }
	                
	                mQcount = 0;
	            }
	        }
        }
    }
    
    /**
     * ID to lookup cannot be EMPTY or "."(JSON DEFAULT) 
     * @param idToFind
     * @return
     */
    private boolean validateIdToFind(String idToFind) {
    	boolean result = false;
    	if (idToFind!=null && !idToFind.equals(".") && !idToFind.equals("")) {
    		result = true;
    	}
    	
    	return result;
    }
    
    protected History copyAppend(History history, String result){
		History clone = (History) history.clone();
		clone.add(result);    	
		return clone;    
    }
    
    
    public int getJsonpos() {
        return mJsonpos;
    }

    public void setJsonpos(int jsonpos) {
        this.mJsonpos = jsonpos;
    }
    
    
}
