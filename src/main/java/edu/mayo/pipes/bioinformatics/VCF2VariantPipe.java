package edu.mayo.pipes.bioinformatics;

import com.google.gson.Gson;
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

import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.bioinformatics.vocab.Type;
import edu.mayo.pipes.exceptions.InvalidPipeInputException;
import edu.mayo.pipes.history.ColumnMetaData;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.util.GenomicObjectUtils;
import java.text.ParseException;
import java.util.Arrays;
import java.util.logging.Level;
import org.apache.log4j.Priority;

/**
 * <b>INPUT:</b>	History that contains 8 columns that correspond to the VCF 4.0 format.
 * 					Assumes the first 8 columns in the history are VCF related.
 * 
 * </br>
 * 
 * <b>OUTPUT:</b>	JSON object string is appended to the end of the history as a new column.
 * 
 * @see http://www.1000genomes.org/wiki/analysis/vcf4.0
 * @see http://phd.chnebu.ch/index.php/Variant_Call_Format_(VCF)
 * 
 */
public class VCF2VariantPipe extends AbstractPipe<History,History> {
	
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
        private static final int COL_FORMAT = 8;
	
	// 8 required fixed fields.  all VCF 4.0+ files should have these
	private static final String[] COL_HEADERS = 
		{"CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO"};
        private static final String NUMBER_SUPPORTING_SAMPLES = "NUMBER_SAMPLES"; //the number of samples that have a given variant
    
    /*
     	From VCF 4.0 format specification:
     	
			INFO fields should be described as follows (all keys are required):

    		##INFO=<ID=ID,Number=number,Type=type,Description=description>

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
    
    // maps a given INFO field ID to an InfoFieldMeta object
    private Map<String, InfoFieldMeta> mFieldMap = new HashMap<String, InfoFieldMeta>();
    //Private variables to hold the rest of the "schema" specific to this VCF file. 
    private HashMap<String,Integer> sampleKeys = new HashMap();
    private HashMap<String,Boolean> formatKeys = new HashMap();
    
    private boolean isHeaderProcessed = false;
    
    // number of data line (does not include header lines)
    private int mDataLineNumber = 0;
    
    public VCF2VariantPipe() {    	
    }
    
    private boolean processSamples = false;
    public VCF2VariantPipe(boolean includeSamples){
        processSamples = true;
    }
    
    /**
     * Processes the VCF header for the INFO column's metadata per field.
     */
    private void processHeader(List<String> headerLines){
        for (String row: headerLines) {
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
    protected History processNextStart() throws NoSuchElementException, InvalidPipeInputException {
        History history = this.starts.next();

        //sLogger.debug("VCF2VariantPipe (before): " + history);
		//sLogger.debug("VCF2VariantPipe (header): " + History.getMetaData().getColumnHeaderRow("\t"));

        
        // record the data line we are going to process
        mDataLineNumber++;
        
        // initialize header only once, on the 1st time through this method
        if (isHeaderProcessed == false) {
        	processHeader(History.getMetaData().getOriginalHeader());
        	
        	ColumnMetaData cmd = new ColumnMetaData(getClass().getSimpleName());
        	History.getMetaData().getColumns().add(cmd);
        	
        	isHeaderProcessed = true;
        }

        // check to make sure we have the required minimum # of columns
        if(history.size() < COL_HEADERS.length){
        	final int requiredColCount = COL_HEADERS.length;
        	final int actualColCount = history.size();
        	
        	StringBuilder sb = new StringBuilder();
        	sb.append("Invalid VCF data line at data line # %s.\n");
        	sb.append("The VCF format requires %s fixed fields per data line, but found only %s field(s).\n");
        	sb.append("Make sure the VCF file has the necessary %s VCF fields delimited by TAB characters.\n");
        	sb.append("Invalid VCF line content: \"%s\"");
        	
        	String errorMesg = String.format(
        							sb.toString(),
        							String.valueOf(mDataLineNumber),
        							requiredColCount,
        							actualColCount,
        							requiredColCount,
        							history.getMergedData("\t")
        						);
        	
        	throw new InvalidPipeInputException(errorMesg, this);
      	}

        // transform into JSON
        String json = buildJSON(history);
        
        history.add(json);
        //sLogger.debug("VCF2VariantPipe (after): " + history);
        
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
        root.addProperty(COL_HEADERS[COL_CHROM],  history.get(COL_CHROM).trim());
        root.addProperty(COL_HEADERS[COL_POS],    history.get(COL_POS).trim());
        root.addProperty(COL_HEADERS[COL_ID],     history.get(COL_ID).trim());
        root.addProperty(COL_HEADERS[COL_REF],    history.get(COL_REF).trim());
        root.addProperty(COL_HEADERS[COL_ALT],    history.get(COL_ALT).trim());
        root.addProperty(COL_HEADERS[COL_QUAL],   history.get(COL_QUAL).trim());
        root.addProperty(COL_HEADERS[COL_FILTER], history.get(COL_FILTER).trim());
        
        // parse and shred INFO column
        JsonObject info = buildInfoJSON(history.get(COL_INFO).trim());
        root.add(COL_HEADERS[COL_INFO], info);
        
        // add core attributes to be used by downstream pipes
        addCoreAttributes(root, history);
        
        // if we should process the samples, then parse the sample info and add it to the JSON
        if(processSamples){
            try {
                addSamples(root, history);
            } catch (ParseException ex) {
                sLogger.log(Priority.ERROR, ex);//todo: we need to log this better, can't remember the right way
            }
        }
        
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

    	// used where an INFO field is not defined in the header
    	// in these special cases, treat as a string
    	InfoFieldMeta defaultMeta = new InfoFieldMeta();
    	defaultMeta.id = "not_defined";
    	defaultMeta.number = 1;
    	defaultMeta.type = INFO_TYPE.String;
    	
    	JsonObject info = new JsonObject();
    	
    	for (String field: infoCol.split(";")) {

    		if (field.indexOf('=') != -1) {
        		int firstEq = field.indexOf('=');

        		String id = field.substring(0, firstEq);
        		String value = field.substring(firstEq + 1);
        		
        		InfoFieldMeta meta = defaultMeta;
        		if (mFieldMap.containsKey(id)) {
        			meta = mFieldMap.get(id);
        		}        		
        		        		
        		if ((meta.number == null) || (meta.number > 1)) {

   	    			// not sure if there are 1 or more, assume array to be safe
        			JsonArray arr = new JsonArray();
        			for (String s: value.split(",")) {
            	    	switch (meta.type) {
            	    	case Integer:
            				if (!isMissingValue(s)) {
            					arr.add(new JsonPrimitive(Integer.parseInt(s.trim())));
            				}
                			break;
            	    	case Float:
            				if (!isMissingValue(s)) {
            					arr.add(new JsonPrimitive(Float.parseFloat(s.trim())));
            				}
            	    		break;
            	    	case Character:
            	    	case String:
            	    		arr.add(new JsonPrimitive(s));
            	    		break;
            	    	}
        			}
        			if (arr.size() > 0) {
        				info.add(id, arr);
        			}
        			
        		} else if (meta.number == 1) {
        			
        	    	switch (meta.type) {
        	    	case Integer:
        				if (!isMissingValue(value)) {
        					info.addProperty(id, Integer.parseInt(value.trim()));
        				}
            			break;
        	    	case Float:    	    		
        				if (!isMissingValue(value)) {
        					info.addProperty(id, Float.parseFloat(value.trim()));
        				}
        	    		break;
        	    	case Character:
        	    	case String:
            			info.addProperty(id, value);
        	    		break;
        	    	}
        			
        		}        		
    		} else if (field.length() > 0) {
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

    	//guaranteed to be unique, if no then perhaps bug
    	String accID = history.get(COL_ID).trim();
    	root.addProperty(CoreAttributes._id.toString(), accID);
    	
    	root.addProperty(CoreAttributes._type.toString(), Type.VARIANT.toString());
    	
    	String chr = GenomicObjectUtils.computechr(history.get(COL_CHROM).trim());
    	root.addProperty(CoreAttributes._landmark.toString(), chr);
    	
    	String refAllele = history.get(COL_REF).trim();
    	root.addProperty(CoreAttributes._refAllele.toString(), refAllele);
    	
    	JsonArray altAlleles = new JsonArray();
    	for (String allele: al(history.get(COL_ALT).trim())) {
    		altAlleles.add(new JsonPrimitive(allele));
    	}
    	root.add(CoreAttributes._altAlleles.toString(), altAlleles);
    	
        if (history.get(COL_POS) != null) {
        	String pos = history.get(COL_POS).trim();
            int minBP = new Integer(pos);        
            int maxBP = new Integer(minBP + history.get(COL_REF).trim().length() - 1);        	

            root.addProperty(CoreAttributes._minBP.toString(), minBP);
        	root.addProperty(CoreAttributes._maxBP.toString(), maxBP);
        }
    }

    private String[] al(String raw){
        List<String> finalList = new ArrayList<String>();
        if(raw.contains(",")){
            //sLogger.debug(raw);
            String[] split = raw.split(",");
            for(int i = 0; i<split.length; i++){
                finalList.add(split[i]);
            }
        }else{
            finalList.add(raw);
        }
        return (String[]) finalList.toArray( new String[0] ); //finalList.size()
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
     * Determines whether the given value represents a "missing" value.  It is
     * common to use a '.' character to designate a value that is missing in
     * structured columns such as ALT or for fields in the INFO column.
     * 
     * @param value The value to check
     * @return true if the value is missing
     */
    private boolean isMissingValue(String value) {
    	String trimVal = value.trim();
		if (trimVal.equals(".")) {
			return true;
		} else {
			return false;
		}				
    }
    
    /**
     * Metadata about an INFO field.
     */
    class InfoFieldMeta {
    	String id;
    	Integer number; // null if it varies, is unknown, or is unbounded
    	INFO_TYPE type;    	
    }
    
    /**
     * Adds the sample information to the given JSON object.
     * 
     * @param root JSON object to add to.
     * @param history Data row from VCF.
     */
    public boolean firstSample = true;
    private void addSamples(JsonObject root, List<String> history) throws ParseException {
        String[] tokens;
        if(firstSample){
            String format = History.getMetaData().getColumns().get(COL_FORMAT).getColumnName();
            if(!format.contains("FORMAT")){
                //if we don't have a format column, sorry, we can't process the sample data, just return
                return;
            }
            firstSample = false;
        }
        tokens = history.get(COL_FORMAT).split(":");
        
        //make sure all the format tokens are in the metadata hash
        for(String tok : tokens){
            this.formatKeys.put(tok, true);
        }
        
        //start at the first sample column (format +1) and go until the end of the array.
        for(int i=COL_FORMAT+1; i<history.size(); i++){
            String col = History.getMetaData().getColumns().get(i).getColumnName();
            parseSample(history.get(i), root, col, tokens);
            //make sure that col is in the metadata hash
            this.sampleKeys.put(col, i+1);
        } 
        
        
        
        
    }
 
    private int findGT(String[] t){
        for(int i=0; i<t.length; i++){
            if(t[i].equalsIgnoreCase("GT")){
                return i;
            }
        }
        return -1;
    }
    
    /**
     * 
     * @param genotype e.g. "./././././.", "0/0/0/0/0/0", "1/1/1/1/1/1"
     * @return 
     */
    public boolean sampleHasVariant(String genotype){
        String s1 = genotype.replaceAll("\\.", "");
        String s2 = s1.replaceAll("0", "");
        String s3 = s2.replaceAll("\\|", "");
        String s4 = s3.replaceAll("/", "");
        if(s4.length() > 0){
            return true;
        }else {
            return false;
        }
    } 
    
    public static boolean isNumeric(String str)
    {
        return str.matches("-?\\d+(\\.\\d+)?");  //match a number with optional '-' and decimal.
    }
    
    /**
     * parse the sample and add it
     * sample: the data for the sample e.g. 
     */
    public void parseSample(String sampleID, JsonObject root, String sampleName, String[] tokens) throws ParseException{
        //System.out.println(Arrays.toString(tokens));
        //System.out.println(sampleName);
        //System.out.println(sampleID);
        String[] split = sampleID.split(":");
        if(split.length > tokens.length){
            throw new ParseException("VCF2VariantPipe.parseSample: the number of tokens in the format field (" + tokens.length + ") and the number of tokens in the sample (" + split.length + ") do not agree. \nFORMAT:"+Arrays.toString(tokens)+"\nSAMPLE: "+sampleID+"\n", 0);
        }
        //find the index of "GT" in the format column
        int GTPosition = findGT(tokens);
        JsonObject genotype = new JsonObject();
        for(int i=0; i<split.length; i++){
            if(split[i].contains(",")){ //it is an array
                String[] arr = split[i].split(",");
                JsonArray jarr = new JsonArray();
                for(int j=0;j<arr.length;j++){
                    //it is a list of numbers
                    if(isNumeric(arr[j])){
                        double d = Double.parseDouble(arr[j].trim()); 
                        jarr.add(new JsonPrimitive(d));
                    //list of strings, add them...   
                    }else{
                        jarr.add(new JsonPrimitive(arr[j]));
                    }
                }
                genotype.add(tokens[i], jarr);
            }else if(isNumeric(split[i])){ //it is not
                genotype.addProperty(tokens[i], Double.parseDouble(split[i]));
            }else { //it is a string, so just add it as a string.
                genotype.addProperty(tokens[i], split[i]);
            }
        }
        
        //now add
        if(GTPosition > -1){
            if(sampleHasVariant(split[GTPosition])){//if the sample GT value (e.g. "0/0/0/0/1/0" contains something other than /,.,|,or 0 (, not included)
                //insert GenotypePositive if the sample has the variant.
                genotype.addProperty("GenotypePositive", 1);
            }
        }
        
        root.add(sampleName, genotype);
        
        //System.out.println(genotype.toString());
        //System.out.println(root.toString());

    }

    public Map<java.lang.String, InfoFieldMeta> getmFieldMap() {
        return mFieldMap;
    }

    public HashMap<java.lang.String, Integer> getSampleKeys() {
        return sampleKeys;
    }

    public HashMap<java.lang.String, Boolean> getFormatKeys() {
        return formatKeys;
    }
    
    public JsonObject getJSONMetadata(){
        JsonObject json = new JsonObject();
        JsonObject info = new JsonObject();
        JsonObject format = new JsonObject();
        JsonObject samples = new JsonObject();
        for(String key : this.mFieldMap.keySet()){
            //System.out.println(key);
            InfoFieldMeta value = mFieldMap.get(key);
            JsonObject meta = new JsonObject();
            //meta.addProperty("id", value.id);
            meta.addProperty("number", value.number);
            meta.addProperty("type", value.type.toString());
            info.add(key, meta);
        }
        for(String key : this.formatKeys.keySet()){
            format.addProperty(key, 1);
        }
        for(String key : this.sampleKeys.keySet()){
            samples.addProperty(key, sampleKeys.get(key));
        }
        //System.out.println(info.toString());
        json.add("INFO", info);
        json.add("FORMAT", format);
        json.add("SAMPLES", samples);
        return json;
    }

}