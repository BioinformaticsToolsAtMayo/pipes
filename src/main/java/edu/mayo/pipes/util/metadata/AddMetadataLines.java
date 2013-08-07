package edu.mayo.pipes.util.metadata;

import java.io.IOException;
import java.util.*;

import edu.mayo.pipes.bioinformatics.vocab.Undefined;
import edu.mayo.pipes.history.ColumnMetaData;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.util.PropertiesFileUtil;

public class AddMetadataLines {


    /**
     * controlled vocabulary for ##BIOR lines
     */
    public static enum BiorMetaControlledVocabulary {
        /** bior prefix */
        BIOR("bior."),
        /** OPERATION and DATATYPE will be used on every command (but not data source related) */
        OPERATION("Operation"),
        DATATYPE("DataType"),
        /** KEY and DESCRIPTION are used only by bior_drill to lookup the column properties */
        FIELD("Field"),
        FIELDDESCRIPTION("FieldDescription"),
        DESCRIPTION("Description"),
        /** These are used by all command including bior_drill */
        SHORTNAME("ShortUniqueName"),
        SOURCE("Source"),
        VERSION("Version"),
        BUILD("Build"),
        PATH("Path"),
        COLUMNPROPERTIES("ColumnProperties"),
        DATASOURCEPROPERTIES("DataSourceProperties");

        private String aKey;

        private BiorMetaControlledVocabulary(String aKey) {
            this.aKey = aKey;
        }

        @Override
        public String toString() {
            return aKey;
        }
    };

	
	public AddMetadataLines() {		
	}

    /**
     * checks the canidate ID to see if it is unique, if it is not unique, then it 'increments' it
     * @param h
     * @param canidate - what we would like the uniqueID to be
     */
    public String getID(History h, String canidate){
        List<String> lines = History.getMetaData().getOriginalHeader();
        HashMap<String,LinkedHashMap> hm = new HashMap<String, LinkedHashMap>();
        for(int i = 0; i< lines.size(); i++){
            String line = lines.get(i);
            if(line.startsWith("##BIOR")){
                LinkedHashMap props = this.parseHeaderLine(line);
                hm.put(props.get("ID").toString(), props);
            }
        }
        String canidate2 = canidate;
        LinkedHashMap props = hm.get(canidate);
        for(Integer numberOfFails = 2; props != null; numberOfFails++){
            canidate2 = canidate + "." + numberOfFails;
            props = hm.get(BiorMetaControlledVocabulary.BIOR + canidate2);
        }
        return canidate2;
    }


    /**
     * For a given catalog, parses the .datasource.properties file and returns a hashmap of they key-value pairs in the file
     *
     * @param catalogPath - the complete path to the catalog that we want to parse the Datasource properties file from
     * @return
     */
    public LinkedHashMap<String,String> parseDatasourceProperties(String catalogPath, LinkedHashMap<String,String> attributes) throws IOException {

        String rootpath = catalogPath;
        if(catalogPath.contains("tsv.bgz")){
            String[] split = catalogPath.split(".tsv.bgz");
            rootpath = split[0];
        }

        PropertiesFileUtil propsUtil = new PropertiesFileUtil(rootpath + ".datasource.properties");


        String catalogShortUniqueName = propsUtil.get(BiorMetaControlledVocabulary.SHORTNAME.toString());
        String catalogSource = propsUtil.get(BiorMetaControlledVocabulary.SOURCE.toString());
        String description = propsUtil.get(BiorMetaControlledVocabulary.DESCRIPTION.toString());
        String catalogVersion = propsUtil.get(BiorMetaControlledVocabulary.VERSION.toString());
        String catalogBuild = propsUtil.get(BiorMetaControlledVocabulary.BUILD.toString());

        attributes.put(BiorMetaControlledVocabulary.SHORTNAME.toString(), catalogShortUniqueName);
        attributes.put(BiorMetaControlledVocabulary.SOURCE.toString(), catalogSource);
        attributes.put(BiorMetaControlledVocabulary.DESCRIPTION.toString(), description);
        attributes.put(BiorMetaControlledVocabulary.VERSION.toString(), catalogVersion);
        attributes.put(BiorMetaControlledVocabulary.BUILD.toString(), catalogBuild);
        attributes.put(BiorMetaControlledVocabulary.PATH.toString(), catalogPath);

        return attributes;
    }

    public Properties parseColumnProperties(String catalogPath) throws IOException {
        String rootpath = catalogPath;
        if(rootpath.endsWith("properties")){
            //then use it directly;
        }else {
            //if it is the catalog, then try to use it
            if(catalogPath.contains("tsv.bgz")){
                String[] split = catalogPath.split(".tsv.bgz");
                rootpath = split[0];
            }
        }

        PropertiesFileUtil propsUtil = new PropertiesFileUtil(rootpath + ".columns.properties");

        return propsUtil.getProperties();
    }


    public History constructMetadataLine(History history, String columnName, String catalogFile, String operation) throws IOException {
        return constructMetadataLine(history, columnName, catalogFile, operation, "JSON"); //by default it will be a JSON column because so many operations plop json on the end of the history
    }

    /**
     * constructMetadataLine basically plops a metadata line with all of the metadata
     * Given a metadata file like this:
     * #  CatalogShortUniqueName will appear in vcf output files and may be duplicated many times per line - so keep as short as possible
     # It should also not conflict with any other catalog (in either BioR or user space)
     # The CatalogShortUniqueName should only have alpha-numeric characters or '_' (A-Z, 0-9, '_').
     # No dots allowed as they will be used as separators with any column names.
     CatalogShortUniqueName=dbSNP137
     CatalogDescription=dbSNP version 137, Patch 10, Human
     CatalogSource=dbSNP
     CatalogVersion=137
     CatalogBuild=GRCh37.p10
     Then create this:
     * ##BIOR=<ID=bior.dbSNP137,Operation="bior_same_variant",DataType="JSON",CatalogShortUniqueName="dbSNP137",CatalogSource="dbSNP",CatalogVersion="137",CatalogBuild="GRCh37.p10",CatalogPath="/data5/bsi/catalogs/bior/v1/dbSNP/137/00-All-GRCh37.tsv.bgz">
     * @param history
     * @param columnName
     * @param catalogFile
     * @param operation - e.g. same_variant, overlap, lookup ect.
     * @return
     */
	public History constructMetadataLine(History history, String columnName, String catalogFile, String operation, String dataType) throws IOException {
		String datasourceName = null;

        LinkedHashMap<String,String> attributes = new LinkedHashMap();
	    List<String> metadataLine = new ArrayList<String>();
		
		if (columnName.contains("bior")) {
			
    		String[] colNameSplit = columnName.split("\\.");
    		
    		if (colNameSplit.length >= 2) {
    			
    			datasourceName = colNameSplit[1];
    			//System.out.println("ColumnName="+column_name+"; DatasourceName="+datasourceName);
                attributes.put("ID", getID(history, columnName));
                attributes.put(BiorMetaControlledVocabulary.OPERATION.toString(), operation);
                attributes.put(BiorMetaControlledVocabulary.DATATYPE.toString(), dataType);
                attributes = this.parseDatasourceProperties(catalogFile, attributes);

                List<String> head = history.getMetaData().getOriginalHeader();
                head.add(head.size()-1, buildHeaderLine(attributes));

    		} 
    	}
		return history; 
	}

    /**
     * For commands like vcf_to_tjson, where there is no catalog, we need to construct the ##BIOR line using this method.
     * it makes a line like this:
     * ##BIOR=<ID=BIOR.vcf_to_tjson,Operation="vcf_to_tjson",DataType="JSON">
     * @param h
     * @param operation
     * @return
     */
    public History constructToJsonLine(History h, String operation, String operationType){
        LinkedHashMap<String,String> attributes = new LinkedHashMap();
        attributes.put("ID", getID(h,BiorMetaControlledVocabulary.BIOR + operationType));
        attributes.put(BiorMetaControlledVocabulary.OPERATION.toString(), operation);
        attributes.put(BiorMetaControlledVocabulary.DATATYPE.toString(), "JSON");
        attributes.put(BiorMetaControlledVocabulary.SHORTNAME.toString(), operationType);
        List<String> head = h.getMetaData().getOriginalHeader();
        head.add(head.size()-1, buildHeaderLine(attributes));
        return h;
    }

    /**
     * for commands like overlap/lookup when the metadata is defined, put the ##BIOR line in the history.
     * @param h
     * @param catalogPath
     * @param operation
     * @return col - the value you should use for the new column you wish to construct in the calling function (modify the history)
     * @throws IOException
     */
    public String constructQueryLine(History h, String catalogPath, String operation) throws IOException {
        LinkedHashMap<String,String> temp = new LinkedHashMap();
        LinkedHashMap<String,String> attributes = new LinkedHashMap();
        parseDatasourceProperties(catalogPath, temp);
        attributes.put("ID",getID(h,BiorMetaControlledVocabulary.BIOR + temp.get(BiorMetaControlledVocabulary.SHORTNAME.toString())));
        attributes.put(BiorMetaControlledVocabulary.OPERATION.toString(), operation);
        attributes.put(BiorMetaControlledVocabulary.DATATYPE.toString(), "JSON");
        for( String key : temp.keySet()){
            attributes.put(key, temp.get(key));
        }
        List<String> head = h.getMetaData().getOriginalHeader();
        head.add(head.size()-1, buildHeaderLine(attributes));
        return attributes.get("ID").substring(5); //remove .bior for consistency
    }

    /**
     * When there is no datasource.properties file, then we need to construct a line like this:
     * "##BIOR=<ID=\"bior.00-All_GRCh37\",Operation=\"bior_lookup\",DataType=\"JSON\",CatalogPath=\"some/file/that/does/not/exist/00-All_GRCh37.tsv.bgz\">"
     * @param h
     * @param catalogPath
     * @param operation
     * @return
     */
    public String constructQueryLineOnNoDatasourceProperties(History h, String catalogPath, String operation){
        LinkedHashMap<String,String> attributes = new LinkedHashMap();
        String[] split = catalogPath.split("/");
        String filename = split[split.length-1];
        String substituteShort = (filename).replaceAll(".tsv.bgz","");
        attributes.put("ID",getID(h,BiorMetaControlledVocabulary.BIOR.toString() + substituteShort));
        attributes.put(BiorMetaControlledVocabulary.OPERATION.toString(), operation);
        attributes.put(BiorMetaControlledVocabulary.DATATYPE.toString(), "JSON");
        attributes.put(BiorMetaControlledVocabulary.SHORTNAME.toString(), substituteShort);
        //attributes.put()
        attributes.put(BiorMetaControlledVocabulary.PATH.toString(), catalogPath);
        List<String> head = h.getMetaData().getOriginalHeader();
        int addline = head.size()-1;
        if(addline == -1){
            History.getMetaData().getOriginalHeader().add(buildHeaderLine(attributes));
        }else {
            head.add(head.size() - 1, buildHeaderLine(attributes));
        }
        return attributes.get("ID").substring(5); //remove .bior for consistency
    }

    /**
     *
     * @param h                      - the history that we need to change
     * @param operation              - the name of the tool called
     * @param datasourcepath         - path to the property file for the tool
     * @param columnpath             - path to the column property file
     * @return   shortName for use on the column
     */
    public String constructToolLine(History h, String datasourcepath, String columnpath, String operation) throws IOException {
        PropertiesFileUtil props = new PropertiesFileUtil(datasourcepath);
        LinkedHashMap<String,String> attributes = new LinkedHashMap();
        attributes.put("ID", getID(h,BiorMetaControlledVocabulary.BIOR + props.get(BiorMetaControlledVocabulary.SHORTNAME.toString())));
        attributes.put(BiorMetaControlledVocabulary.OPERATION.toString(), operation);
        attributes.put(BiorMetaControlledVocabulary.DATATYPE.toString(), "JSON");
        attributes.put(BiorMetaControlledVocabulary.SHORTNAME.toString(), props.get(BiorMetaControlledVocabulary.SHORTNAME.toString()));
        attributes.put(BiorMetaControlledVocabulary.DESCRIPTION.toString(), props.get(BiorMetaControlledVocabulary.DESCRIPTION.toString()));
        attributes.put(BiorMetaControlledVocabulary.VERSION.toString(), props.get(BiorMetaControlledVocabulary.VERSION.toString()));
        attributes.put(BiorMetaControlledVocabulary.BUILD.toString(),props.get(BiorMetaControlledVocabulary.BUILD.toString()));
        attributes.put(BiorMetaControlledVocabulary.DATASOURCEPROPERTIES.toString(), datasourcepath);
        attributes.put(BiorMetaControlledVocabulary.COLUMNPROPERTIES.toString(), columnpath);
        List<String> head = h.getMetaData().getOriginalHeader();
        head.add(head.size()-1, buildHeaderLine(attributes));
        return attributes.get("ID").substring(5); //remove .bior for consistency
    }

    /**
     * This bit of logic is used all over the pipes project.  Hopefully this function can help centralize it somewhat.
     * Basically if a column, c, specified is:
     * c=0 -> runtime exception we can't modify the zero column because it does not exist yet
     * c>0 -> c = c - input.size() -1
     * c<0 -> c = c
     *
     * then the logic to get the column later is get(input.size + c)
     *
     * @param h
     * @param columnNumber
     * @return
     */
    public int fixDrillRow(History h, int columnNumber){
        int col = -1;
        //ensure that we are dealing with a negative column
        if(columnNumber > 0){
            col = columnNumber - h.size() -1;
        } else if (columnNumber == 0){
            throw	new RuntimeException("You can't specify column number 0, use negative or positive numbers only!");
        }
        return col;
    }

    /**
     *
     * construct something like this:
     * ##BIOR=<ID="bior.dbSNP137.INFO.SSR",Operation="DrillPipe",DataType="STRING",Field="INFO.SSR",FieldDescription="Variant suspect reason code (0 - unspecified, 1 - paralog, 2 - byEST, 3 - Para_EST, 4 - oldAlign, 5 - other)",ShortUniqueName="dbSNP137",Source="dbSNP",Description="dbSNP from NCBI",Version="137",Build="GRCh37.p10",Path="src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz">
     * and add it to the history
     * @param h              -
     * @param columnNumber   - the column number we are drilling ; this will be used to figure out the columnName
     * @param drillPaths     - used to count and name the columns that are produced by the drill
     * @return
     */
    public String constructDrillLines(History h, String operation, int columnNumber, String[] drillPaths) {
        List<ColumnMetaData> hcol = History.getMetaData().getColumns();
        int col = fixDrillRow(h, columnNumber);

        ColumnMetaData cmd = hcol.get(hcol.size() + col);
        String cmeta = cmd.getColumnName();
        int pos = getHistoryMetadataLine4HeaderValue(cmeta);
        if(pos == -1){
            return cmeta; //could not find the column we need to drill, adding metadata failed
        }else {
            String preLine = History.getMetaData().getOriginalHeader().get(pos).toString();
            for(String path: drillPaths){
                putDrillMetaLines(h, operation, preLine, path);
            }
        }


        return cmeta.substring(5); //make sure to remove the bior. for consistency across the functions
    }

    /**
     *
     * @param h
     * @param preLine - the header line that describes the JSON column we are drilling -- many of it's attributes will be copied
     * @param dpath   - String for the drill path
     */
    private void putDrillMetaLines(History h, String operation, String preLine, String dpath){
        HashMap<String,String> datasourceattr = parseHeaderLine(preLine);
        String catalogPath = datasourceattr.get(BiorMetaControlledVocabulary.PATH.toString());
        LinkedHashMap<String,String> attributes = new LinkedHashMap<String, String>();
        attributes.put("ID",getID(h,BiorMetaControlledVocabulary.BIOR + datasourceattr.get(BiorMetaControlledVocabulary.SHORTNAME.toString()) + "." + dpath));
        attributes.put(BiorMetaControlledVocabulary.OPERATION.toString(), operation);
        attributes.put(BiorMetaControlledVocabulary.DATATYPE.toString(), "STRING");
        attributes.put(BiorMetaControlledVocabulary.FIELD.toString(), dpath);
        try {
            //attributes = parseDatasourceProperties(datasourceattr.get(BiorMetaControlledVocabulary.PATH.toString()), attributes);
            Properties properties;
            if(datasourceattr.get(BiorMetaControlledVocabulary.PATH.toString()) != null){
                properties = parseColumnProperties(datasourceattr.get(BiorMetaControlledVocabulary.PATH.toString()));
            }else {
                properties = parseColumnProperties(datasourceattr.get(BiorMetaControlledVocabulary.DATASOURCEPROPERTIES.toString()));
            }

            attributes.put(BiorMetaControlledVocabulary.FIELDDESCRIPTION.toString(), (String) properties.get(dpath));
        } catch (IOException e) {
            //else there is not a columns.properties file, so we can't add a description
            attributes.put(BiorMetaControlledVocabulary.FIELDDESCRIPTION.toString(), "");
        }
        //copy all of the properties from the catalog to the ##BIOR-drill-line
        for( String key : datasourceattr.keySet()){
            if(     key.equalsIgnoreCase("ID") ||
                    key.equalsIgnoreCase(BiorMetaControlledVocabulary.DATATYPE.toString()) ||
                    key.equalsIgnoreCase(BiorMetaControlledVocabulary.OPERATION.toString()) ||
                    key.equalsIgnoreCase(BiorMetaControlledVocabulary.FIELD.toString()) ||
                    key.equalsIgnoreCase(BiorMetaControlledVocabulary.FIELDDESCRIPTION.toString())
                    ){
                ;   // do nothing
            }else { // add it
                    attributes.put(key, datasourceattr.get(key));
            }
        }
        List<String> head = h.getMetaData().getOriginalHeader();
        head.add(head.size()-1, buildHeaderLine(attributes));
    }

    /**
     * given the name of a header that contains JSON data, drill needs to know what metadata line coresponds to it
     * This method will find the line number and return it.
     * @param headerValue     some column name e.g. bior.ID
     * @return line number for header
     */
    public int getHistoryMetadataLine4HeaderValue(String headerValue){
        List<String> header = History.getMetaData().getOriginalHeader();
        int i =0;
        for(String line : header){
            if(line.startsWith("##BIOR")){
                LinkedHashMap<String, String> attr = this.parseHeaderLine(line);
                if(attr.get("ID").equalsIgnoreCase(headerValue))
                    return i;
            }
            i++;
        }
        return -1;
    }



    /**
     * if the operation is drill, then in the header we want to do something like this instead:
     * ##BIOR=<ID=bior.dbSNP137.INFO.SSR,Operation="bior_drill",DataType="String",Key="INFO.SSR",Description="Variant suspect reason code (0 - unspecified, 1 - paralog, 2 - byEST, 3 - Para_EST, 4 - oldAlign, 5 - other)",CatalogShortUniqueName="dbSNP137",CatalogSource="dbSNP",CatalogVersion="137",CatalogBuild="GRCh37.p10",CatalogPath="/data5/bsi/catalogs/bior/v1/dbSNP/137/00-All-GRCh37.tsv.bgz">
     *
     * @param history
     * @param columnName
     * @return
     */
    public History constructDrillLine(History history, String columnName) throws IOException {
        String datasourceName = null;
        String datasourceColumnName = null;  //what the datasource calls the column
        String operation = "DrillPipe";

        LinkedHashMap<String, String> phead = null;
        int biorline = -1; //by default there is no line and we can add no metadata.
        //find the line in the History that the column came from
        if (columnName.contains("bior")) {
            String[] colNameSplit = columnName.split("\\.");

            if (colNameSplit.length >= 2) {
                datasourceName = colNameSplit[1];

                //construct the drill path for the column by replacing the datasource with nothing
                datasourceColumnName = columnName.replaceAll(".*"+datasourceName + "\\.", "");

                for(int i = 0; i< History.getMetaData().getOriginalHeader().size(); i++){
                    String headerLine = History.getMetaData().getOriginalHeader().get(i);
                    if(headerLine.startsWith("##BIOR")){
                        phead = parseHeaderLine(headerLine); //the parsed header
                        if(phead.get(BiorMetaControlledVocabulary.SHORTNAME.toString()).equalsIgnoreCase(datasourceName)){
                            biorline = i;
                            i =  History.getMetaData().getOriginalHeader().size() + 1; //break loop
                        }
                    }
                }
            }
        }


        if(biorline == -1){
            return history;
        }else {
            //parse the column.properties file to get a description for the key if it exists
            Properties prop = parseColumnProperties(phead.get(BiorMetaControlledVocabulary.PATH.toString()));
            //for each drill path, append another ##BIOR column to the header

            String description = "";
            description = (String) prop.get(datasourceColumnName);
            if(description == null){
                description = "";
            }

            LinkedHashMap<String,String> attributes = new LinkedHashMap();
            attributes.put("ID", getID(history, columnName));
            attributes.put(BiorMetaControlledVocabulary.OPERATION.toString(), operation);
            attributes.put(BiorMetaControlledVocabulary.DATATYPE.toString(), "STRING");
            attributes.put(BiorMetaControlledVocabulary.FIELD.toString(), datasourceColumnName);
            attributes.put(BiorMetaControlledVocabulary.DESCRIPTION.toString(), description);
            attributes = this.parseDatasourceProperties(phead.get(BiorMetaControlledVocabulary.PATH.toString()), attributes);

            List<String> head = history.getMetaData().getOriginalHeader();
            head.add(head.size()-1, buildHeaderLine(attributes));

        }


        return history;
    }

	
	
	/**
	 * Builds lines like 
	 * 	##BIOR=<ID=bior.column_name,Operation="command that was run",DataType=JSON/String >
	 *  ##BIOR=<ID=bior.VCF2VariantPipe,Operation="bior_vcf_to_variant",DataType="JSON">
	 *	##BIOR=<ID=bior.dbSNP137,Operation="bior_same_variant",DataType="JSON",CatalogShortUniqueName="dbSNP137",CatalogSource="dbSNP",CatalogVersion="137",CatalogBuild="GRCh37.p10",CatalogPath="/data5/bsi/catalogs/bior/v1/dbSNP/137/00-All-GRCh37.tsv.bgz">
	 *	##BIOR=<ID=bior.dbSNP137.INFO.SSR,Operation="bior_drill",DataType="String",Key="INFO.SSR",Description="Variant suspect reason code (0 - unspecified, 1 - paralog, 2 - byEST, 3 - Para_EST, 4 - oldAlign, 5 - other)",CatalogShortUniqueName="dbSNP137",CatalogSource="dbSNP",CatalogVersion="137",CatalogBuild="GRCh37.p10",CatalogPath="/data5/bsi/catalogs/bior/v1/dbSNP/137/00-All-GRCh37.tsv.bgz">
	 * @param attributes
	 * @return
	 */
	public String buildHeaderLine(LinkedHashMap<String,String> attributes) {
		StringBuilder sb = new StringBuilder();
		sb.append("##BIOR=<");
		
		String delim="";
		for(String key : attributes.keySet()) {
            String value = attributes.get(key);
            String kv = key + "=\"" + value + "\"";
			sb.append(delim).append(kv);
			delim = ",";
		}
		sb.append(">");
		
		return sb.toString();
	}

    /**
     * take a line like:
     * ##BIOR=<ID="bior.dbSNP137",CatalogShortUniqueName="dbSNP137",CatalogSource="dbSNP",CatalogVersion="137",CatalogBuild="GRCh37.p10">
     * and construct a hashmap out of it
     * @param line
     * @return
     */
    public LinkedHashMap<String,String> parseHeaderLine(String line){
        LinkedHashMap hm = new LinkedHashMap();
        String half = line.replaceFirst("##BIOR=<", "");
        String removeTrailingGreaterThan = half.substring(0, half.length() - 1);
        String[] split = removeTrailingGreaterThan.split(",");
        //for each key-value pair
        StringBuilder sb = new StringBuilder();
        for(int i=0; i< split.length; i++){
            while(!split[i].endsWith("\"")){
                sb.append(split[i]);
                sb.append(",");
                i++;
            }
            sb.append(split[i]);
            parseChunk(sb.toString(), hm);
            sb = new StringBuilder();
        }
        return hm;
    }

    /**
     *
     * @param s   - something like: Build="GRCh37.p10" this will get parsed to a k-v pair
     * @param hm  key-value pair added to this hash
     */
    private void parseChunk(String s, HashMap<String,String> hm){
        int idx = s.indexOf("=");
        if(idx < 0) return; //failed to parse pair, don't add anything
        String key = s.substring(0,idx);
        String value = s.substring(idx+2,s.length()-1);
        hm.put(key,value);
    }


		
}
