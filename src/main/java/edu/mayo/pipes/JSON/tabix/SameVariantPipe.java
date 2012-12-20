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
    
    public SameVariantPipe(String tabixDataFile, int historyPostion) throws IOException {
        super(tabixDataFile, historyPostion);
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
    public SameVariantPipe(String tabixDataFile, boolean isRsidCheckOnly, boolean isAlleleCheckOnly, int historyPostion) throws IOException{
        super(tabixDataFile, historyPostion);
        this.comparableObject = new SameVariantLogic(isRsidCheckOnly, isAlleleCheckOnly);
    }
    

    private class SameVariantLogic implements ComparableObjectInterface {
        private boolean isRsidCheckOnly = false;//user says you can only compare on rsids...
        private boolean isAlleleCheckOnly = false; //user says you can only compare on alleles
        private JsonPath chrJsonPath = null;
        private JsonPath minBpJsonPath = null;
        private JsonPath rsIdJsonPath = null;
        private JsonPath refJsonPath = null;
        private JsonPath altJsonPath = null;
        private Gson gson = new Gson();
        
        public SameVariantLogic(){
            init();
        }

        private SameVariantLogic(boolean rsidCheckOnly, boolean alleleCheckOnly) {
            isRsidCheckOnly = rsidCheckOnly;
            isAlleleCheckOnly = alleleCheckOnly;
            init();
        }
        
        public void init(){
        	chrJsonPath = JsonPath.compile(CoreAttributes._landmark.toString());
        	minBpJsonPath = JsonPath.compile(CoreAttributes._minBP.toString());
        	rsIdJsonPath = JsonPath.compile(CoreAttributes._id.toString());
        	refJsonPath = JsonPath.compile(CoreAttributes._refAllele.toString());
        	altJsonPath = JsonPath.compile(CoreAttributes._altAlleles.toString());
        }

        /**
         * 
         * @param jsonIn  - input variant (e.g. from the user)
         * @param jsonOut - variant from the tabix file / database
         * @return true if they are the 'same' false otherwise
         */
        @Override
        public boolean same(String jsonIn, String jsonOut) {
            //landmarks must be the same...
            String chrIn  = chrJsonPath.read(jsonIn);
            String chrOut = chrJsonPath.read(jsonOut);
            if(chrIn == null || chrIn.length()==0 || ! chrIn.equalsIgnoreCase(chrOut)){
                return false;        
            }
            
            //minbp must be the same
            Integer minBpIn  = minBpJsonPath.read(jsonIn);
            Integer minBpOut = minBpJsonPath.read(jsonOut);
            //System.out.println(minbpIN + ":" + minbpOUT);
            if(minBpIn == null || minBpIn.compareTo(minBpOut) != 0) {
                return false;
            }
            
            String rsIdIn  = rsIdJsonPath.read(jsonIn);
            String rsIdOut = rsIdJsonPath.read(jsonOut);
            String refIn   = refJsonPath.read(jsonIn);
            String refOut  = refJsonPath.read(jsonOut);
            ArrayList<String> altsIn   = toList((JSONArray)altJsonPath.read(jsonIn));
            ArrayList<String> altsOut  = toList((JSONArray)altJsonPath.read(jsonOut));
            boolean isRsIdMatch = rsIdIn != null && rsIdIn.length() > 0 && rsIdIn.equalsIgnoreCase(rsIdOut);
            boolean isRefAlleleMatch = refIn  != null && refIn.length() > 0 && refIn.equalsIgnoreCase(refOut);
            boolean isAltAlleleMatch = altsIn != null && altsIn.size() > 0 && isSubset(altsIn, altsOut);
            
            if( isRsidCheckOnly ) 
            	return isRsIdMatch;
            else if( isAlleleCheckOnly ) 
            	return isRefAlleleMatch && isAltAlleleMatch;
            else {
            	return isRsIdMatch || (isRefAlleleMatch && isAltAlleleMatch);
            }
        }
        
        /** Make sure all items in altsIn are contained within altsOut */
        private boolean isSubset(ArrayList<String> subset, ArrayList<String> allItems) {
        	for(String item : subset) {
        		if(! allItems.contains(item))
        			return false;
        	}
        	return true;
        }
        
        private ArrayList<String> toList(JSONArray jsonArray) {
        	ArrayList<String> list = new ArrayList<String>();
        	for(int i=0; i < jsonArray.size(); i++) 
        		list.add((String)jsonArray.get(i));
			return list;
        }
    }
    
}
