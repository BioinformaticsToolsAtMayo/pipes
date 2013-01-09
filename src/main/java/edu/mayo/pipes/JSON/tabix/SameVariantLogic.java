/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import com.google.gson.Gson;
import com.jayway.jsonpath.JsonPath;
import edu.mayo.pipes.bioinformatics.vocab.ComparableObjectInterface;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import java.util.ArrayList;
import net.minidev.json.JSONArray;

/**
 *
 * @author m102417
 */
    public class SameVariantLogic implements ComparableObjectInterface {
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

        public SameVariantLogic(boolean rsidCheckOnly, boolean alleleCheckOnly) {
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
         * @param jsonCatalog - variant from the tabix file / database
         * @return true if they are the 'same' false otherwise
         */
        @Override
        public boolean same(String jsonIn, String jsonCatalog) {
            //landmarks must be the same...
            String chrIn  = chrJsonPath.read(jsonIn);
            String chrOut = chrJsonPath.read(jsonCatalog);
            if(chrIn == null || chrIn.length()==0 || ! chrIn.equalsIgnoreCase(chrOut)){
                return false;        
            }
            
            //minbp must be the same
            Integer minBpIn  = minBpJsonPath.read(jsonIn);
            Integer minBpOut = minBpJsonPath.read(jsonCatalog);
            //System.out.println(minbpIN + ":" + minbpOUT);
            if(minBpIn == null || minBpIn.compareTo(minBpOut) != 0) {
                return false;
            }
            
            String rsIdIn  = rsIdJsonPath.read(jsonIn);
            String rsIdOut = rsIdJsonPath.read(jsonCatalog);
            String refIn   = refJsonPath.read(jsonIn);
            String refOut  = refJsonPath.read(jsonCatalog);
            ArrayList<String> altsIn   = toList((JSONArray)altJsonPath.read(jsonIn));
            ArrayList<String> altsOut  = toList((JSONArray)altJsonPath.read(jsonCatalog));
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
        public boolean isSubset(ArrayList<String> subset, ArrayList<String> allItems) {
        	for(String item : subset) {
        		if(! allItems.contains(item))
        			return false;
        	}
        	return true;
        }
        
        public ArrayList<String> toList(JSONArray jsonArray) {
        	ArrayList<String> list = new ArrayList<String>();
        	for(int i=0; i < jsonArray.size(); i++) 
        		list.add((String)jsonArray.get(i));
			return list;
        }
    }
    
