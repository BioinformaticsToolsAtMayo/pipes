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
import com.google.gson.JsonElement;
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
 *  http://www.1000genomes.org/wiki/analysis/vcf4.0
 *  http://phd.chnebu.ch/index.php/Variant_Call_Format_(VCF)
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
    	
    	A regular expression is used to extract 4 pieces of information:

    		1. ID 		(regex grouping #1)
    		2. Number	(regex grouping #2)
    		3. Type		(regex grouping #3)
                4. Description  (regex grouping #4)
    */
    private final String mRegexStr = ".+" + "ID=([^,]+)" + ".+"+ "Number=([^,]+)" + ".+" + "Type=(Integer|Float|Flag|Character|String)" + ".+" + "Description=([^,]+)" + ".+";
    private static final int REGEX_GRP_ID   = 1;
    private static final int REGEX_GRP_NUM  = 2;
    private static final int REGEX_GRP_TYPE = 3;
    private static final int REGEX_DESCRIPTION = 4;
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
    
    private boolean allSamples = false;
    private boolean processSamples = false;
    public VCF2VariantPipe(boolean includeSamples){
        processSamples = true;      
    }
    
    public VCF2VariantPipe(boolean includeSamples, boolean AllSamples){
        processSamples = true;
        this.allSamples = AllSamples;
    }
    
    /**
     * Processes the VCF header for the INFO column's metadata per field.
     */
    private void processHeader(List<String> headerLines){
        for (String row: headerLines) {
        	Matcher m = mRegexPattern.matcher(row);
        	if (m.find()) {
        		
        		InfoFieldMeta meta = new InfoFieldMeta();
                meta.entryType = getEntryType(row);
        		
        		// pattern matched, extract groups
        		meta.id = m.group(REGEX_GRP_ID);
        		meta.type = INFO_TYPE.fromString(m.group(REGEX_GRP_TYPE));
        		try {
        			meta.number = Integer.parseInt(m.group(REGEX_GRP_NUM));
        		} catch (NumberFormatException nfe) {
        			meta.number = null;
        		}
        		meta.desc = m.group(REGEX_DESCRIPTION).replaceAll("\"", "");
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
        JsonObject info = buildInfoJSON(history.get(COL_INFO).trim(), history);
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

    public String reformat(List<String> line){
        StringBuilder sb = new StringBuilder();
        for(String s : line){
            sb.append(s);
            sb.append("\t");
        }
        String r = sb.toString();
        return r.substring(0,r.length()-2);
    }
    /**
     * Examines the INFO column and shreds it into a JSON friendly structure based
     * on INFO field metadata mined from the VCF header.
     * 
     * @param infoCol The INFO column
     * @return JSON object
     */
    private JsonObject buildInfoJSON(String infoCol, List<String> dataLine) {

    	// used where an INFO field is not defined in the header
    	// in these special cases, treat as a string
    	InfoFieldMeta defaultMeta = new InfoFieldMeta();
    	defaultMeta.id = "not_defined";
    	defaultMeta.number = 1;
    	defaultMeta.type = INFO_TYPE.String;
        defaultMeta.entryType = "INFO";
    	
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
                                try {
                                    arr.add(new JsonPrimitive(Integer.parseInt(s.trim())));
                                } catch (Exception e){

                                    System.err.println("Invalid VCF Line: " + reformat(dataLine));
                                }
            				}
                			break;
            	    	case Float:
            				if (!isMissingValue(s)) {
                                try {
            					    arr.add(new JsonPrimitive(Float.parseFloat(s.trim())));
                                }catch (Exception e){
                                    System.err.println("Invalid VCF Line: " + reformat(dataLine));
                                }
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
                            try {
                                info.addProperty(id, Integer.parseInt(value.trim()));
                            }catch (Exception e){
                                System.err.println("Invalid VCF Line: " + reformat(dataLine));
                            }

        				}
            			break;
        	    	case Float:    	    		
        				if (!isMissingValue(value)) {
                            try {
                                info.addProperty(id, Float.parseFloat(value.trim()));
                            }catch (Exception e){
                                System.err.println("Invalid VCF Line: " + reformat(dataLine));
                            }

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
        String desc; //description of the field as found in the VCF
    	Integer number; // null if it varies, is unknown, or is unbounded
    	INFO_TYPE type;
        String entryType; //e.g. INFO, FILTER, FORMAT...
    }

    /**
     input: a VCF header line e.g.
     ##FILTER=<ID=q10,Description="Quality below 10">
     return - the entityType e.g. FILTER
     other valid entity types are INFO, FORMAT, source, reference, ...
     */
    public String getEntryType(String line){
        if(!line.startsWith("##")) return "";
        if(line.contains("=")){
            String[] split = line.split("=");
            return split[0].substring(2);
        }
        else return ""; //unknown
    }
    
    /**
     * Adds the sample information to the given JSON object.
     * 
     * @param root JSON object to add to.
     * @param history Data row from VCF.
     */
    public boolean firstSample = true;
    private void addSamples(JsonObject root, List<String> history) throws ParseException {
        this.GenotypePostitiveCount = 0;
        this.GenotypePositiveSamples = new JsonArray();
        this.ALLAD = new ArrayList<Double>();
        this.ALLPL = new ArrayList<Double>();
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
        
        JsonArray samples = new JsonArray();
        //start at the first sample column (format +1) and go until the end of the array.
        for(int i=COL_FORMAT+1; i<history.size(); i++){
            String col = History.getMetaData().getColumns().get(i).getColumnName();
             
            parseSample(history.get(i), samples, col, tokens);
            //make sure that col is in the metadata hash
            this.sampleKeys.put(col, i+1);
        } 
        root.add("samples", samples);
        root.addProperty("GenotypePostitiveCount", this.GenotypePostitiveCount);
        root.add("GenotypePositiveList", this.GenotypePositiveSamples);
        //PLIntervalMin = 0;
        root.addProperty("PLIntervalMin", min(this.ALLPL));
        //PLIntervalMax = Double.MAX_VALUE;
        root.addProperty("PLIntervalMax", max(this.ALLPL));
        //PLAverage = Double.MAX_VALUE;
        root.addProperty("PLAverage", average(this.ALLPL));
        //ADIntervalMin = 0;
        root.addProperty("ADIntervalMin", min(this.ALLAD));
        //ADIntervalMax = Double.MAX_VALUE;
        root.addProperty("ADIntervalMax", max(this.ALLAD));
        //ADAverage = Double.MAX_VALUE;
        root.addProperty("ADAverage", average(this.ALLAD));       
        
    }
 
    private int findGT(String[] t){
        return findT(t, "GT");
    }
    private int findPL(String[] t){
        return findT(t, "PL");
    }
    private int findAD(String[] t){
        return findT(t, "AD");
    }
    
    private int findT(String[] t, String tok){
        for(int i=0; i<t.length; i++){
            if(t[i].equalsIgnoreCase(tok)){
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
    
    /**
     * 
     * @param genotype e.g. "./././././.", "./.", "." then return true
     * else return false
     * @return 
     */
    public boolean sampleHasNoVariantData(String genotype){
        if(genotype.startsWith(".")){
            return true;
        }else {
            return false;
        }
    }
    
    public static boolean isNumeric(String str)
    {
        return str.matches("-?\\d+(\\.\\d+)?");  //match a number with optional '-' and decimal.
    }
    
    private int GenotypePostitiveCount = 0;
    private JsonArray GenotypePositiveSamples = new JsonArray();
    private ArrayList<Double> ALLPL = new ArrayList<Double>();
    private ArrayList<Double> ALLAD = new ArrayList<Double>();
    /**
     * parse the sample and add it
     * sample: the data for the sample e.g. 
     */
    public void parseSample(String sampleID, JsonArray samples, String sampleName, String[] tokens) throws ParseException{
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
        //go through the format columns
        for(int i=0; i<split.length; i++){
            if(split[i].contains(",")){ //it is an array
                String[] arr = split[i].split(",");
                JsonArray jarr = new JsonArray();
                ArrayList<Double> values = new ArrayList<Double>();
                for(int j=0;j<arr.length;j++){
                    //it is a list of numbers
                    if(isNumeric(arr[j])){
                        double d = Double.parseDouble(arr[j].trim()); 
                        jarr.add(new JsonPrimitive(d));
                        values.add(d);
                    //list of strings, add them...   
                    }else{
                        jarr.add(new JsonPrimitive(arr[j]));
                    }
                }
                genotype.add(tokens[i], jarr); 
                //calculate the maxPL and add
                if(tokens[i].equals("PL")){
                    genotype = addMaxMinPL(genotype,values);
                }
                //calculate the minAD/maxAD and add to the json
                //for the first, AD that is ref and all of the others are alternates
                //pick the min and max from the alternates.
                if(tokens[i].equals("AD")){
                    genotype = addMinMaxAD(genotype,values);
                }
            }else if(isNumeric(split[i])){ //it is not
                genotype.addProperty(tokens[i], Double.parseDouble(split[i]));
            }else { //it is a string, so just add it as a string.
                genotype.addProperty(tokens[i], split[i]);
            }
        }
        
        //now add GenotypePositive for those samples that contain the variant in the GT string
        if(GTPosition > -1){
            if(sampleHasVariant(split[GTPosition])){//if the sample GT value (e.g. "0/0/0/0/1/0" contains something other than /,.,|,or 0 (, not included)
                //insert GenotypePositive if the sample has the variant.
                genotype.addProperty("GenotypePositive", 1);
                this.GenotypePostitiveCount++;
                JsonPrimitive prim = new JsonPrimitive(sampleName);
                this.GenotypePositiveSamples.add( prim );
            }
        }
        genotype.addProperty("sampleID", sampleName);
        
        if(this.allSamples == true){
            samples.add(genotype);
        }else {
            if(GTPosition > -1){
                if(sampleHasNoVariantData(split[GTPosition])){
                    //don't add it to the JSON Array, it is just wasteful
                }else{
                    samples.add(genotype);
                }
            }
        }
        
        
        //System.out.println(genotype.toString());
        //System.out.println(root.toString());

    }
    
    /**
     * PL : the phred-scaled genotype likelihoods rounded to the closest integer (and otherwise defined precisely as the GL field) (Integers)
     * we need to select the max and of the values from this array and insert as a seperate value for use in filtering
     */
    public JsonObject addMaxMinPL(JsonObject genotype, ArrayList<Double> values){
            //System.out.println(values);
            double minPL = Double.MAX_VALUE;
            double maxPL = -Double.MAX_VALUE;
            for(int i=0;i<values.size();i++){
                Double d = values.get(i);
                this.ALLPL.add(d);
                if(d > maxPL){
                    maxPL = d;
                }
                if(d < minPL){
                    minPL = d;
                }
            }
            //System.out.println(maxPL);
            genotype.addProperty("maxPL", maxPL);
            genotype.addProperty("minPL", minPL);
            //System.out.println(genotype.toString());
            return genotype;
    }
    
    public JsonObject addMinMaxAD(JsonObject genotype, ArrayList<Double> values){
        double maxAD = -Double.MAX_VALUE;
        double minAD = Double.MAX_VALUE;
        for(int i=1;i<values.size();i++){
            Double d = values.get(i);
            this.ALLAD.add(d);
            if(d > maxAD){
                maxAD = d;
            }
            if(d < minAD){
                minAD = d;
            }
        }
        //System.out.println(maxPL);
        genotype.addProperty("maxAD", maxAD);
        genotype.addProperty("minAD", minAD);
        //System.out.println(genotype.toString());
        return genotype;
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
            meta.addProperty("Description", value.desc);
            meta.addProperty("EntryType", value.entryType);
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
    
    public static double min(ArrayList<Double> m){
        double min = Double.MAX_VALUE;
        for(double d : m){
            if(d<min) min = d;
        }
        return min;
    }
    
    public static double max(ArrayList<Double> m){
        double max = Double.NEGATIVE_INFINITY;
        for(double d : m){
            if(d>max) max = d;
        }
        return max;
    }
    
    public static double average(ArrayList<Double> m) {
        double sum = 0;
        for (double d : m) {
            sum += d;
        }
        if (m.size() == 0) return Double.NaN;
        return sum / m.size();
    }
    
    // the array double[] m MUST BE SORTED
    public static double median(double[] m) {
        int middle = m.length/2;
        if (m.length%2 == 1) {
            return m[middle];
        } else {
            return (m[middle-1] + m[middle]) / 2.0;
        }
    }



}