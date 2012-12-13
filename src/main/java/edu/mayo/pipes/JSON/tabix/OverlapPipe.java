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
  
// Original overlap pipe code, refactored and moved up to TabixParentPipe    
//    @Override
//    protected History processNextStart() throws NoSuchElementException {
//        setup();
//        //If the search has another result, append the result to the history
//        if(search.hasNext()){
//            //System.out.println("Next Search Result...");
//            qcount++;
//            String result = (String) search.next();
//            return copyAppend(history, result);
//        }else {//otherwise, the search did not have any more results, get the next history
//            if(qcount == 0){//we did not have any search results, append empty JSON to the history and send it along
//                qcount++; //just to get the history to reset on the next call
//                return copyAppend(history,"{}");//return empty result
//            }else {//we did have at least one result (perhaps empty).. and they are all done
//                history = this.starts.next();
//                //reset the pipeline for the search query
//                search.reset(); 
//                search.setStarts(Arrays.asList(history.get(history.size()-1)));
//                qcount = 0;
//                //and start pulling data again...
//                return processNextStart();
//            }
//            
//        }
//    }
    
    


    

    
}
