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
    private LinkedList<Integer> posqueue = new LinkedList<Integer>();
    protected History history = null;
    protected int qcount;
    protected boolean isFirst = true;
    protected ComparableObjectInterface comparableObject;
    protected int historyPos = -1; //position in the history to look for the input to the transform (default the last column)
    private File bgzipFile;
    
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
    }
    
    public List<String> getIDs(List<History> hs, int col){
        List<String> ids = new ArrayList<String>();
        for(int i=0; i<hs.size(); i++){
            History h = hs.get(i);
            ids.add(h.get(col));
        }
        return ids;
    }
    
     

    FindIndex fi = new FindIndex();
    IndexUtils iutil = new IndexUtils();
    List<History> histQueue = new LinkedList<History>();
    @Override
    public History processNextStart() throws NoSuchElementException {
        History h = null;
        while(this.starts.hasNext()){
            h = this.starts.next();
            histQueue.add(h);
        }
        try {        
            
            String id = h.get(0);
            HashMap<String,List<Long>> idxMap = fi.find(Arrays.asList(id), true, dbConn);
            HashMap<String, List<String>> zipLinesByIndex = iutil.getZipLinesByIndex(bgzipFile,idxMap);
            h.add(zipLinesByIndex.toString());
        } catch (IOException e) {
            Logger.getLogger(LookupPipe.class.getName()).log(Level.SEVERE, null, e);
        } catch (SQLException ex) {
            Logger.getLogger(LookupPipe.class.getName()).log(Level.SEVERE, null, ex);
        }
    //    throw new UnsupportedOperationException("Not supported yet.");
        //1. get the ID from the history
        //2. use the ID to find the positions in h2 where the ID exists
        //3. for each index in the file, get the data and return it. fan out.
        //HashMap<String,List<String>> key2LinesMap = utils.getZipLinesByIndex(bgzipFile, key2posMap);
        return h;
    }
    
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
    
//    @Override
//    protected History processNextStart() throws NoSuchElementException {
//    	// Setup only on first row
//    	setup();
//    	
//        while(true){
//	        //If the search has another result, append the result to the history
//	        if(search.hasNext()){
//	            //System.out.println("Next Search Result...");
//	            if(valid(comparableObject)){
//	                qcount++;
//	                return copyAppend(history, validResult);
//	            }else {//not a valid result, try again...
//	                //return processNextStart(); --loop again
//	            }         
//	        }else {//otherwise, the search did not have any more results, get the next history
//	            if(qcount == 0){//we did not have any search results, append empty JSON to the history and send it along
//	                qcount++; //just to get the history to reset on the next call
//	                return copyAppend(history,"{}");//return empty result
//	            }else {//we did have at least one result (perhaps empty).. and they are all done
//	                history = this.starts.next();
//	                //reset the pipeline for the search query
//	                search.reset(); 
//	                search.setStarts(Arrays.asList(history.get(history.size()+historyPos)));
//	                qcount = 0;
//	                //and start pulling data again...
//	                //return processNextStart();
//	            }
//	        }
//        }
//    }

    
    
    
//    	public void database() throws ClassNotFoundException, SQLException, IOException {
//		System.out.println("=================================");
//		System.out.println("Perform database search...");
//		double start = System.currentTimeMillis();
//		IndexerDatabase idxDb = new IndexerDatabase();
//		Connection dbConn = idxDb.getConnectionH2(dbIndexFile);
//		HashMap<String,List<Long>> idxMap = idxDb.findIndexes(ids, false, dbConn);
//                
//		double end = System.currentTimeMillis();
//		System.out.println("Runtime for database search: " + (end-start)/1000.0);
//		//long memUse = new IndexUtils().getMemoryUse() / (1024*1024);
//		System.out.println("  Num matching keys: " + idxMap.size());
//		//System.out.println("  Mem use (MBs):  " + memUse);
//	}
    
}
