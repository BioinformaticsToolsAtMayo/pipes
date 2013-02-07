/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics.sequence;

import com.jayway.jsonpath.JsonPath;
import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.JSON.tabix.TabixParentPipe;
import edu.mayo.pipes.JSON.tabix.TabixReader;
import edu.mayo.pipes.JSON.tabix.TabixSearchPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.history.History;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author dquest
 * Input is a genomic interval in the format (as an ArrayList)
 * _landmark    _minBP  _maxBP
 * _minBP is 1 based!
 * 
 * The output is:
 * _landmark    _minBP  _maxBP  sequence
 * e.g.
 * 22    1000    1010    agtccagtta
 * _landmark in this example is chr22.
 * 
 * In general, this pipe will ignore any columns beyond the last 3.  e.g.
 * foo  bar baz 22    1000    1010
 * is the same as
 * 22    1000    1010
 * 
 */
public class Bed2SequencePipe extends AbstractPipe<ArrayList<String>,ArrayList<String>> {

    TabixSearchPipe mTabixSearch;
    private int mMaxBpCol = 0;
    private boolean mIsUseJsonCol = false;
    
    private JsonPath mChromJsonPath;
    private JsonPath mMinBpJsonPath;
    private JsonPath mMaxBpJsonPath;

    
    public Bed2SequencePipe(String tabixDataFile) throws IOException {
        mTabixSearch = new TabixSearchPipe(tabixDataFile);
    }
    
    /**
     * 
     * @param tabixDataFile
     * @param column - the column to find _landmark _minBP and _maxBP (default 0)
     * e.g. if the data looks like
     * X    1234    1456    A
     * Then you may want column = -1
     * if the data looks like:
     * X    1234    1456    A   {"foo":"veep"}
     * Then you may want column = -2
     * @throws IOException 
     */
    public Bed2SequencePipe(String tabixDataFile, int column) throws IOException {
        mTabixSearch = new TabixSearchPipe(tabixDataFile);
        this.mMaxBpCol = column;
    }
    
    /**
     * Use the JSON column, which should be the last one
     * @param tabixDataFile
     * @param isUseJson - Use the json column.  If true, uses the last column for JSON
     *        If false, the last column should be the maxBP column
	 * Data should look like this:
     *        X    1234    1456    {"_landmark":"X","_minBP":1234,"_maxBP":1456,.....}
     * @throws IOException 
     */
    public Bed2SequencePipe(String tabixDataFile, boolean isUseJson) throws IOException {
        mTabixSearch = new TabixSearchPipe(tabixDataFile);
        mMaxBpCol = 0;
        mIsUseJsonCol = isUseJson;
        mChromJsonPath = JsonPath.compile(CoreAttributes._landmark.toString());
        mMinBpJsonPath = JsonPath.compile(CoreAttributes._minBP.toString());
        mMaxBpJsonPath = JsonPath.compile(CoreAttributes._maxBP.toString());
    }


    int start = 0;
    int end = 0;
    String result = "";
    TabixReader.Iterator records;
    @Override
    public ArrayList<String> processNextStart() throws NoSuchElementException {
        try {
            ArrayList<String> history = this.starts.next();
            
            String tabixQuery = getTabixQueryString(history);
            String subsequence = getSequence(tabixQuery);
            history.add( subsequence );
            
            return history;
        } catch (IOException ex) {
            throw new NoSuchElementException();//perhaps bad??
        }
    }
    
    /** Get the one-based subsequence that matches the range in the query */
    private String getSequence(String tabixQuery) throws NumberFormatException, IOException {
    	StringBuilder subsequence = new StringBuilder();
        records = mTabixSearch.tquery(tabixQuery);
        String rec = null;
        boolean isFirst = true;
        int seqEndPos = 0;
        // Loop thru the records that overlap the tabixQuery range and concatenate them together
        while( (rec = records.next()) != null ) {
            String[] split = rec.split("\t");
            String seq = split[3];
            // Trim off the part of the subsequence that comes before the query start
            if(isFirst) {
            	int numToTrimOffFront = getMin(tabixQuery) - Integer.parseInt(split[1]);
        		seq = seq.substring(numToTrimOffFront);
        		isFirst = false;
        	}
            subsequence.append(seq);
            // Save the end position for later so we know how much to trim from end
            seqEndPos = Integer.parseInt(split[2]);
        }
        // Now, trim off the last part that is past the query end
        int numToTrimOffEnd = seqEndPos - getMax(tabixQuery);
        return subsequence.substring(0, subsequence.length()-numToTrimOffEnd);
    }
    
    /** Ex:  "17:12345-23456" */
    private String getTabixQueryString(ArrayList<String> history) {
    	String query = "";
    	if( mIsUseJsonCol ) {
    		String lastCol = history.get(history.size()-1);
    		String chr = mChromJsonPath.read(lastCol);
    		int min = mMinBpJsonPath.read(lastCol);
    		int max = mMaxBpJsonPath.read(lastCol);
    		query = chr + ":" + min + "-" + max;
    	} else {
    		String chr = history.get(history.size()-3+this.mMaxBpCol);
    		String min = history.get(history.size()-2+this.mMaxBpCol);
    		String max = history.get(history.size()-1+this.mMaxBpCol);
    		query = chr + ":" + min + "-" + max;
    	}
    	return query;
    }
    
    private int getMin(String query) {
    	return Integer.parseInt(query.substring(query.indexOf(":") + 1, query.indexOf("-")));
    }
    
    private int getMax(String query) {
    	return Integer.parseInt(query.substring(query.indexOf("-") + 1));
    }
    
    
}
