/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import com.google.gson.JsonArray;
import com.jayway.jsonpath.JsonPath;
import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.Pipe;
import edu.mayo.pipes.bioinformatics.vocab.ComparableObjectInterface;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.history.History;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
        private boolean rsidCheckOnly = false;//user says you can only compare on rsids...
        private boolean alleleCheckOnly = false; //user says you can only compare on alleles
        private JsonPath landmark = null;
        private JsonPath minBP = null;
        private JsonPath id = null;
        private JsonPath ref = null;
        private JsonPath alt = null;    

        public SameVariantLogic(){
            init();
        }
        
        public void init(){
            landmark = JsonPath.compile(CoreAttributes._landmark.toString());
            minBP = JsonPath.compile(CoreAttributes._minBP.toString());
            id = JsonPath.compile(CoreAttributes._id.toString());
            ref = JsonPath.compile(CoreAttributes._refAllele.toString());
            alt = JsonPath.compile(CoreAttributes._altAlleles.toString());
        }

        /**
         * 
         * @param a - input variant (e.g. from the user)
         * @param b - variant from the tabix file / database
         * @return true if they are the 'same' false otherwise
         */
        @Override
        public boolean same(String jsonIN, String jsonOUT) {
            //landmarks must be the same...
            String lmrkIN = landmark.read(jsonIN);
            String lmrkOUT = landmark.read(jsonOUT);
            if(lmrkIN == null || lmrkIN.length()==0 || !lmrkIN.equalsIgnoreCase(lmrkOUT)){
                return false;        
            }
            //minbp must be the same
            Integer minbpIN = minBP.read(jsonIN);
            Integer minbpOUT = minBP.read(jsonOUT);
            //System.out.println(minbpIN + ":" + minbpOUT);
            if(minbpIN == null || minbpIN == minbpOUT){
                return false;
            }

            
            
            
            return false;
        }
        
        /** Check whether variant1's altAllele list is a subset of variant2's */ 
//	private boolean hasSameAltAlleles(JsonArray altsIN, JsonArray altsOUT) {
//		if (!v1.isSetAltAlleleFWD() && !v2.isSetAltAlleleFWD()) {
//			// not set for either objects
//			return true;
//		} else if ((v1.isSetAltAlleleFWD() && v2.isSetAltAlleleFWD()) == false) {
//			// not set for only 1 object
//			return false;
//		} else {
//			// set for both objects, need to dig deeper
//
//			List<String> alts1 = v1.getAltAlleleFWD();
//			List<String> alts2 = v2.getAltAlleleFWD();
//
//			// Make sure all Alt alleles in the 1st variant are contained in the 2nd variant's alt allele list
//			boolean hasAll = true;
//			for(String alt : alts1) {
//				if( ! alts2.contains(alt) )
//					hasAll = false;
//			}
//			return hasAll;
//		}
//	}
        

    }
    
    
}
