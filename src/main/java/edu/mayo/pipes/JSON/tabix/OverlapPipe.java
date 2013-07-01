/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.Pipe;

import edu.mayo.pipes.history.History;

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
public class OverlapPipe extends TabixParentPipe {    

    public OverlapPipe(String tabixDataFile) throws IOException {
        super(tabixDataFile);
    }
    
    public OverlapPipe(String tabixDataFile, int historyPosition) throws IOException {
        super(tabixDataFile, historyPosition);
    }
    
    public OverlapPipe(String tabixDataFile, int minBPExtend, int maxBPExtend) throws IOException{
        super(tabixDataFile);
        search = new TabixSearchPipe(tabixDataFile, minBPExtend, maxBPExtend);
    }    

    public OverlapPipe(String tabixDataFile, int minBPExtend, int maxBPExtend, int historyPosition) throws IOException{
        super(tabixDataFile, historyPosition);
        search = new TabixSearchPipe(tabixDataFile, minBPExtend, maxBPExtend);
        
    }  
    
}
