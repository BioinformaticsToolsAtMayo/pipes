/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import java.io.IOException;
import java.util.ArrayList;

import net.minidev.json.JSONArray;

import com.google.gson.Gson;
import com.jayway.jsonpath.JsonPath;

import edu.mayo.pipes.bioinformatics.vocab.ComparableObjectInterface;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;

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
 * In essence, the pipe (script) would look at the last column in the file (JSON call that v1), 
 * do a lookup in the tabix file for everything that overlaps (v2, v3, v4,).  For each one that overlaps, 
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
    
    public SameVariantPipe(String tabixDataFile, int historyPosition) throws IOException {
        super(tabixDataFile, historyPosition);
        this.comparableObject = new SameVariantLogic();
    }
    
    /**
     * 
     * 
            For a given pair of variants v1,v2:
                CASE1: rsID, chr, and start position match
                CASE2: chr, start position, ref allele, and alt alleles match; alleles match iff 
                            *  - Ref alleles match exactly
                            *  - Alternate alleles from v1 are a subset of v2's
     * 
     * @param tabixDataFile - the catalog
     * @param isRsidCheckOnly - default is false, if true then CASE 1 is the only valid way for two variants to be true.
     * @param isAlleleCheckOnly - default is false, if true then CASE 2 is the only valid way for two variants to be true.
     * @param historyPostion - number of positions to look back (default -1)
     * @throws IOException 
     */
    public SameVariantPipe(String tabixDataFile, boolean isRsidCheckOnly, boolean isAlleleCheckOnly, int historyPosition) throws IOException{
        super(tabixDataFile, historyPosition);
        if( isRsidCheckOnly && isAlleleCheckOnly ){
        	throw new IllegalArgumentException("Cannot set both rsIdOnly AND alleleOnly flags!");
        }
        this.comparableObject = new SameVariantLogic(isRsidCheckOnly, isAlleleCheckOnly);
    }

    public ComparableObjectInterface getSameVariantLogic() {
        return comparableObject;
    }
    

    


}
