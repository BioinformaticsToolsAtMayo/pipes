/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.Pipe;
import edu.mayo.pipes.bioinformatics.vocab.ComparableObjectInterface;
import edu.mayo.pipes.history.History;
import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 *
 * @author dquest
 * Same Variant Builds on top of Overlap Pipe.
 * Overlap takes a list of strings in.  The last string in the list is a JSON string.  
 * It then drills into the JSON String, to get the core attributes it needs:
 * mainly: 
	_landmark,
	_minBP,
	_maxBP,
 * to get back all strings that overlap, it constructs a query with the core attributes.
 * 
 */
public class SameVariantPipe extends TabixParentPipe{
    

    public SameVariantPipe(String tabixDataFile) throws IOException {
        super(tabixDataFile);
        this.comparableObject = new SameVariantLogic();
    }
    

    /**
     * 
     */
    private class SameVariantLogic implements ComparableObjectInterface {

        @Override
        public boolean same(Object a, Object b) {
            return true;
        }

    }
    
    
}
