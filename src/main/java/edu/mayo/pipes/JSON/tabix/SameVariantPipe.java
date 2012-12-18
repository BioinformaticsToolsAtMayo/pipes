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
 * Then it does the followin logic:
 * Those would be variant1 and variant2, the variants that we are attempting to determine if they are the same.  
 * In essence, the pipe (script) would look at the last column in the file (JSON Ð call that v1), 
 * do a lookup in the tabix file for everything that overlaps (v2, v3, v4, É).  For each one that overlaps, 
 * it would also require CASE1 or CASE2 be satisfied.  If they are not, it would dump/filter out the match.  
 * If it did, then it would append the match to the end of the column.  

For a given pair of variants v1,v2:
     CASE1: rsID, chr, and start position match
     CASE2: chr, start position, ref allele, and alt alleles match; alleles match iff 
                *  - Ref alleles match exactly
                *  - Alternate alleles from v1 are a subset of v2's
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
