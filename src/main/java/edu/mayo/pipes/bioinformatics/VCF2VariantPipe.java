package edu.mayo.pipes.bioinformatics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.util.GenomicObjectUtils;

/**
 * <b>INPUT:</b>	History that contains 8 columns that correspond to the VCF 4.0 format.
 * 					Assumes the first 8 columns in the history are VCF related.
 * 
 * </br>
 * 
 * <b>OUTPUT:</b>	JSON object string is appended to the end of the history as a new column.
 * 
 * @see http://www.1000genomes.org/wiki/analysis/vcf4.0
 * 
 */
public class VCF2VariantPipe extends AbstractPipe<List<String>,List<String>>{
    
	private static final Logger sLogger = Logger.getLogger(VCF2VariantPipe.class);
	
	// VCF column ordinals
	private static final int COL_CHROM = 0;
	private static final int COL_POS = 1;
	private static final int COL_ID = 2;
	private static final int COL_REF = 3;
	private static final int COL_ALT = 4;
	private static final int COL_QUAL = 5;
	private static final int COL_FILTER = 6;
	private static final int COL_INFO = 7;
	
	private static final String[] COL_HEADERS = 
		{"CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO"};
	
	// collects the VCF header rows, order is preserved as seen in file
    private List<String> mHeaderRows = new ArrayList<String>();
    
    /*
     	From VCF 4.0 format specification:
     	
			INFO fields should be described as follows (all keys are required):

    		##INFO=<ID=ID,Number=number,Type=type,Description=ÓdescriptionÓ>

    		Possible Types for INFO fields are: Integer, Float, Flag, Character, and String.
    	
    	A regular expression is used to extract 3 pieces of information:

    		1. ID 		(regex grouping #1)
    		2. Number	(regex grouping #2)
    		3. Type		(regex grouping #3)
    */
    private final String mRegexStr = ".+" + "ID=([^,]+)" + ".+"+ "Number=([^,]+)" + ".+" + "Type=(Integer|Float|Flag|Character|String)" + ".+";
    private static final int REGEX_GRP_ID   = 1;
    private static final int REGEX_GRP_NUM  = 2;
    private static final int REGEX_GRP_TYPE = 3;
    private Pattern mRegexPattern = Pattern.compile(mRegexStr);

    private boolean mHeaderInitialized = false;
    
    // maps a given INFO field ID to an InfoFieldMeta object
    private Map<String, InfoFieldMeta> mFieldMap = new HashMap<String, InfoFieldMeta>();
    
    /**
     * Processes the VCF header for the INFO column's metadata per field.
     */
    private void initializeHeader(){
        for (String row: mHeaderRows) {
        	Matcher m = mRegexPattern.matcher(row);
        	if (m.find()) {
        		
        		InfoFieldMeta meta = new InfoFieldMeta();
        		
        		// pattern matched, extract groups
        		meta.id = m.group(REGEX_GRP_ID);
        		meta.type = INFO_TYPE.fromString(m.group(REGEX_GRP_TYPE));
        		try {
        			meta.number = Integer.parseInt(m.group(REGEX_GRP_NUM));
        		} catch (NumberFormatException nfe) {
        			meta.number = null;
        		}
        		
        		mFieldMap.put(meta.id, meta);
        	}
        }
    }

    @Override
    protected List<String> processNextStart() throws NoSuchElementException {
        List<String> history = this.starts.next();
        
        // fast forward and capture header rows until we hit the first data row
        if (mHeaderInitialized == false) {
	    	String commentCol = history.get(0);
	        while(commentCol.startsWith("#")){
	            mHeaderRows.add(commentCol);
	
	            history = this.starts.next();
	        	commentCol = history.get(0);
	        }
	
	        // entire header has been captured, now process it
	        initializeHeader();
	        
	        mHeaderInitialized = true;
        }
                
        if(history.size() < COL_HEADERS.length){
        	throw new NoSuchElementException();
      	}        

        // transform into JSON
        String json = buildJSON(history);
        
        history.add(json);
        
        return history;
    }    
        
    /**
     * Translates the VCF data row into JSON
     * 
     * @param history A single VCF data row
     * @return
     */
    private String buildJSON(List<String> history) {    	
    	
        JsonObject root = new JsonObject();
        
        // carry forward all columns except for INFO verbatim into JSON
        root.addProperty(COL_HEADERS[COL_CHROM],  history.get(COL_CHROM));        
        root.addProperty(COL_HEADERS[COL_POS],    history.get(COL_POS));
        root.addProperty(COL_HEADERS[COL_ID],     history.get(COL_ID));
        root.addProperty(COL_HEADERS[COL_REF],    history.get(COL_REF));
        root.addProperty(COL_HEADERS[COL_ALT],    history.get(COL_ALT));
        root.addProperty(COL_HEADERS[COL_QUAL],   history.get(COL_QUAL));
        root.addProperty(COL_HEADERS[COL_FILTER], history.get(COL_FILTER));
        
        // parse and shred INFO column
        JsonObject info = buildInfoJSON(history.get(COL_INFO));
        root.add(COL_HEADERS[COL_INFO], info);
        
        // add core attributes to be used by downstream pipes
        addCoreAttributes(root, history);
        
        return root.toString();    	
    }
    
    /**
     * Examines the INFO column and shreds it into a JSON friendly structure based
     * on INFO field metadata mined from the VCF header.
     * 
     * @param infoCol The INFO column
     * @return JSON object
     */
    private JsonObject buildInfoJSON(String infoCol) {
    	
    	JsonObject info = new JsonObject();
    	
    	for (String field: infoCol.split(";")) {

    		if (field.indexOf('=') != -1) {
        		int firstEq = field.indexOf('=');

        		String id = field.substring(0, firstEq);
        		String value = field.substring(firstEq + 1);
        		
        		InfoFieldMeta meta = mFieldMap.get(id);
        		if ((meta.number == null) || (meta.number > 1)) {

        			// not sure if there are 1 or more, assume array to be safe
        			JsonArray arr = new JsonArray();
        			for (String s: value.split(",")) {
            	    	switch (meta.type) {
            	    	case Integer:
                			arr.add(new JsonPrimitive(Integer.parseInt(s)));
                			break;
            	    	case Float:    	    		
            	    		arr.add(new JsonPrimitive(Float.parseFloat(s)));
            	    		break;
            	    	case Character:
            	    	case String:
            	    		arr.add(new JsonPrimitive(s));
            	    		break;
            	    	}
        			}
        			info.add(id, arr);
        			
        		} else if (meta.number == 1) {
        			
        	    	switch (meta.type) {
        	    	case Integer:
            			info.addProperty(id, Integer.parseInt(value));
            			break;
        	    	case Float:    	    		
            			info.addProperty(id, Float.parseFloat(value));
        	    		break;
        	    	case Character:
        	    	case String:
            			info.addProperty(id, value);
        	    		break;
        	    	}
        			
        		}        		
    		} else {
    			// dealing with field of type Flag
    			// there is no value
    			info.addProperty(field, true);    			
    		}    		
    	}
    	
    	return info;
    }

    /**
     * Adds core attributes relevant to a variant to the given JSON object.
     * 
     * @param root JSON object to add to.
     * @param history Data row from VCF.
     */
    private void addCoreAttributes(JsonObject root, List<String> history) {
    	// TODO: do we need type???
        //root.addProperty("variant_type", getTypeFromVCF(variant));
        //root.addProperty("type", "Variant");

    	//guaranteed to be unique, if no then perhaps bug
    	String accID = history.get(COL_ID);
    	root.addProperty(CoreAttributes._id.toString(), accID);
    	
    	String chr = GenomicObjectUtils.computechr(history.get(COL_CHROM));
    	root.addProperty(CoreAttributes._landmark.toString(), chr);
    	
    	String refAllele = history.get(COL_REF);
    	root.addProperty(CoreAttributes._refAllele.toString(), refAllele);
    	
    	JsonArray altAlleles = new JsonArray();
    	for (String allele: al(history.get(COL_ALT))) {
    		altAlleles.add(new JsonPrimitive(allele));
    	}
    	root.add(CoreAttributes._altAlleles.toString(), altAlleles);
    	
        if (history.get(COL_POS) != null) {
            int minBP = new Integer(history.get(COL_POS));        
            int maxBP = new Integer(minBP + history.get(COL_REF).length() - 1);        	

            root.addProperty(CoreAttributes._minBP.toString(), minBP);
        	root.addProperty(CoreAttributes._maxBP.toString(), maxBP);
        }
    }
    
//    private String getTypeFromVCF(Variant v){
//        String[] altAllele = v.getAltAllele();
//        if (v.getRefAllele().length() == altAllele[0].length()){
//            return "SNP";
//        }
//        if (v.getRefAllele().length() < altAllele[0].length()){
//            return "insertion";
//        }
//        if (v.getRefAllele().length() > altAllele[0].length()){
//            return "deletion";
//        }
//        else return "unknown";
//    }

    private String[] al(String raw){
        ArrayList finalList = new ArrayList();
        if(raw.contains(",")){
            //System.out.println(raw);
            String[] split = raw.split(",");
            for(int i = 0; i<split.length; i++){
                finalList.add(split[i]);
            }
        }else{
            finalList.add(raw);
        }
        return (String[]) finalList.toArray( new String[0] ); //finalList.size()
    }
    
    private HashMap populate(String[] data){
        HashMap hm = new HashMap();
        for(int i = 0; i<data.length; i++){
            //System.out.println(data[i]);
            if(data[i].contains("=")){
                String[] tokens = data[i].split("=");
                hm.put(tokens[0], tokens[1]);
            }else {
                hm.put(data[i], "1");
            }
        }
        return hm;
    }
    
    /* enumeration to capture Type values efficiently */
    private enum INFO_TYPE {
    	
    	Integer, Float, Flag, Character, String; 
    
    	public static INFO_TYPE fromString(String s) {
    		if (s.equals(Integer.toString())) {
    			return Integer;
    		}
    		else if (s.equals(Float.toString())) {
    			return Float;
    		}
    		else if (s.equals(Flag.toString())) {
    			return Flag;
    		}
    		else if (s.equals(Character.toString())) {
    			return Character;
    		}
    		else if (s.equals(String.toString())) {
    			return String;
    		} else {
    			throw new RuntimeException("Invalid VCF 4.0 type: " + s);
    		}
    	}
    };    
    
    /**
     * Metadata about an INFO field.
     */
    class InfoFieldMeta {
    	String id;
    	Integer number; // null if it varies, is unknown, or is unbounded
    	INFO_TYPE type;
    }
}