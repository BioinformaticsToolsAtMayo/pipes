package edu.mayo.pipes.util.metadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import edu.mayo.pipes.bioinformatics.vocab.Undefined;
import edu.mayo.pipes.history.ColumnMetaData;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.util.PropertiesFileUtil;
import edu.mayo.pipes.util.StringUtils;

public class AddMetadataLines {


    /**
     * controlled vocabulary for ##BIOR lines
     */
    public static enum BiorMetaControlledVocabulary {
        /** bior prefix */
        BIOR("bior."),
        /** ID, OPERATION, and DATATYPE will be used on every command (but not data source related). ID must map to the column header name. */
        ID("ID"),
        OPERATION("Operation"),
        DATATYPE("DataType"),
        /** KEY and DESCRIPTION are used only by bior_drill to lookup the column properties */
        FIELD("Field"),
        FIELDDESCRIPTION("FieldDescription"),
        /** These are used by all command including bior_drill - specific to the catalog or tool */
        SHORTNAME("ShortUniqueName"),
        SOURCE("Source"),
        VERSION("Version"),
        BUILD("Build"),
        NUMBER("Number"), //if the field has one or more values (same as VCF - but multiple values will be a JSON)
        DESCRIPTION("Description"),
        /** Used by bior_compress to denote the delimiter that splits values,
         *  and the escaped delimiter to replace if the delimiter character should occur in the value */
        DELIMITER("Delimiter"),
        ESCAPEDDELIMITER("EscapedDelimiter"),
        // PATH is the full canonical path to a catalog (tsv.bgz file) that has accompanying "datasource" and "columns" properties files 
        PATH("Path"),
        // Since tools will not have a catalog path, we must point to the "datasource" and "properties" files directly to get info about the tool or drilled columns 
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
     * checks the candidate ID to see if it is unique, if it is not unique, then it 'increments' it
     * @param h
     * @param candidate - what we would like the uniqueID to be
     */
    public String getID(History h, String candidate){
        List<String> lines = History.getMetaData().getOriginalHeader();
        HashMap<String,LinkedHashMap> hm = new HashMap<String, LinkedHashMap>();
        for(int i = 0; i< lines.size(); i++){
            String line = lines.get(i);
            if(line.startsWith("##BIOR")){
                LinkedHashMap props = this.parseHeaderLine(line);
                hm.put(props.get(BiorMetaControlledVocabulary.ID.toString()).toString(), props);
            }
        }
        String candidate2 = candidate;
        LinkedHashMap props = hm.get(candidate);
        for(Integer numberOfFails = 2; props != null; numberOfFails++){
            candidate2 = candidate + "." + numberOfFails;
            props = hm.get(BiorMetaControlledVocabulary.BIOR + candidate2);
        }
        return candidate2;
    }



    /**
     * For a given catalog, parses the .datasource.properties file and returns a hashmap of they key-value pairs in the file
     *
     * @param catalogPath - the complete path to the catalog that we want to parse the Datasource properties file from
     * @return
     */
    public LinkedHashMap<String,String> parseDatasourceProperties(String catalogPath, LinkedHashMap<String,String> attributes) throws IOException {
    	PropertiesFileUtil propsUtil; 
    	if (!catalogPath.endsWith(".datasource.properties")) 
    	propsUtil = new PropertiesFileUtil(getDatasourcePropsPath(catalogPath));
        else
        propsUtil = new PropertiesFileUtil(catalogPath);
    	
        String catalogShortUniqueName = propsUtil.get(BiorMetaControlledVocabulary.SHORTNAME.toString());
        String catalogSource = propsUtil.get(BiorMetaControlledVocabulary.SOURCE.toString());
        String description = propsUtil.get(BiorMetaControlledVocabulary.DESCRIPTION.toString());
        String catalogVersion = propsUtil.get(BiorMetaControlledVocabulary.VERSION.toString());
        String catalogBuild = propsUtil.get(BiorMetaControlledVocabulary.BUILD.toString());

        put(attributes, BiorMetaControlledVocabulary.SHORTNAME.toString(), catalogShortUniqueName);
        put(attributes, BiorMetaControlledVocabulary.SOURCE.toString(), catalogSource);
        put(attributes, BiorMetaControlledVocabulary.DESCRIPTION.toString(), description);
        put(attributes, BiorMetaControlledVocabulary.VERSION.toString(), catalogVersion);
        put(attributes, BiorMetaControlledVocabulary.BUILD.toString(), catalogBuild);
        put(attributes, BiorMetaControlledVocabulary.PATH.toString(), catalogPath);

        return attributes;
    }
    
    /** Add only non-null attributes to a HashMap.  Assign values to "" if they are null */
    private void put(LinkedHashMap attributes, String key, String value){
        if(key == null){
            return;
        }
        if(value == null){
            value = "";
        }
        // Replace a double-quote with a backslash-double-quote
        attributes.put(key,value.replaceAll("\"","\\\\\""));
    }


    
    /** Get the path to the columns.tsv file from the full catalogPath */
    private String getColumnsPropsPath(String catalogPath) {
    	if (!catalogPath.endsWith(".columns.tsv"))
    	return getPropsFilePath(catalogPath, ".columns.tsv");
    	else
    		return catalogPath;
    }

    /** Get the path to the datasource.properties file from the full catalogPath */
    private String getDatasourcePropsPath(String catalogPath) {
    	if( ! catalogPath.endsWith(".datasource.properties"))
    		return getPropsFilePath(catalogPath, ".datasource.properties");
    	else
    		return catalogPath;
    }
    
    
    /** Generic method to get the full path to the columns.tsv or datasource.properties files from the full catalog path
     *  @param  catalogPath   The full path to the catalog (.tsv.bgz file)
     *  @param  propsFileExtension   The extension to look for in the properties file path (".columns.tsv" or ".datasource.properties")  */
    private String getPropsFilePath(String catalogPath, final String propsFileExtension) {
    	final String CTLG_EXT = ".tsv.bgz";
    	
    	// If it ends with the extension already, then it is probably the correct file, so use it directly
    	if( catalogPath.endsWith(propsFileExtension) )
    		return catalogPath;
    	// Else, if it ends with the catalog extension, then strip that off and add the props file extension
    	else if( catalogPath.endsWith(CTLG_EXT) )
    		return catalogPath.replace(CTLG_EXT, propsFileExtension);
    	// Else, just tack on the extension on the end of the file path
    	else
    		return catalogPath + propsFileExtension;
    }



    public HashMap<String,ColumnMetaData> parseColumnProperties(String catalogPath) throws IOException {
        ColumnMetaData cmd = new ColumnMetaData("foo");
        HashMap<String,ColumnMetaData> descriptions = cmd.parseColumnProperties(getColumnsPropsPath(catalogPath));
    	return descriptions;
    }


    /**
     * For commands like vcf_to_tjson, where there is no catalog, we need to construct the ##BIOR line using this method.
     * it makes a line like this:
     * ##BIOR=<ID=BIOR.vcf_to_tjson,Operation="vcf_to_tjson",DataType="JSON">
     * @param h
     * @param operation
     * @return
     */
    public History constructToTJsonLine(History h, String operation, String operationType){
        LinkedHashMap<String,String> attributes = new LinkedHashMap();
        put(attributes, BiorMetaControlledVocabulary.ID.toString(), getID(h,BiorMetaControlledVocabulary.BIOR + operationType));
        put(attributes, BiorMetaControlledVocabulary.OPERATION.toString(), operation);
        put(attributes, BiorMetaControlledVocabulary.DATATYPE.toString(),  ColumnMetaData.Type.JSON.toString());
        put(attributes, BiorMetaControlledVocabulary.SHORTNAME.toString(), operationType);
        List<String> head = h.getMetaData().getOriginalHeader();
        if(head.size() > 0){
            head.add(head.size()-1, buildHeaderLine(attributes));
        }else{
            head.add(buildHeaderLine(attributes));
        }
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
        put(attributes, BiorMetaControlledVocabulary.ID.toString(),getID(h,BiorMetaControlledVocabulary.BIOR + temp.get(BiorMetaControlledVocabulary.SHORTNAME.toString())));
        put(attributes, BiorMetaControlledVocabulary.OPERATION.toString(), operation);
        put(attributes, BiorMetaControlledVocabulary.DATATYPE.toString(), ColumnMetaData.Type.JSON.toString());
        for( String key : temp.keySet()){
            put(attributes, key, temp.get(key));
        }
        List<String> head = h.getMetaData().getOriginalHeader();
        head.add(head.size() - 1, buildHeaderLine(attributes));
        return attributes.get(BiorMetaControlledVocabulary.ID.toString()).substring(5); //remove .bior for consistency
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
        put(attributes, BiorMetaControlledVocabulary.ID.toString(),getID(h,BiorMetaControlledVocabulary.BIOR.toString() + substituteShort));
        put(attributes, BiorMetaControlledVocabulary.OPERATION.toString(), operation);
        put(attributes, BiorMetaControlledVocabulary.DATATYPE.toString(), ColumnMetaData.Type.JSON.toString());
        put(attributes, BiorMetaControlledVocabulary.SHORTNAME.toString(), substituteShort);
        put(attributes, BiorMetaControlledVocabulary.PATH.toString(), catalogPath);
        List<String> head = h.getMetaData().getOriginalHeader();
        int addline = head.size()-1;
        if(addline == -1){
            History.getMetaData().getOriginalHeader().add(buildHeaderLine(attributes));
        }else {
            head.add(head.size() - 1, buildHeaderLine(attributes));
        }
        return attributes.get(BiorMetaControlledVocabulary.ID.toString()).substring(5); //remove .bior for consistency
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
        put(attributes, BiorMetaControlledVocabulary.ID.toString(), 				getID(h,BiorMetaControlledVocabulary.BIOR + props.get(BiorMetaControlledVocabulary.SHORTNAME.toString())));
        put(attributes, BiorMetaControlledVocabulary.OPERATION.toString(), 		operation);
        put(attributes, BiorMetaControlledVocabulary.DATATYPE.toString(), 		ColumnMetaData.Type.JSON.toString());
        put(attributes, BiorMetaControlledVocabulary.SHORTNAME.toString(), 		props.get(BiorMetaControlledVocabulary.SHORTNAME.toString()));
        put(attributes, BiorMetaControlledVocabulary.DESCRIPTION.toString(), 	props.get(BiorMetaControlledVocabulary.DESCRIPTION.toString()));
        put(attributes, BiorMetaControlledVocabulary.VERSION.toString(), 		props.get(BiorMetaControlledVocabulary.VERSION.toString()));
        put(attributes, BiorMetaControlledVocabulary.BUILD.toString(),			props.get(BiorMetaControlledVocabulary.BUILD.toString()));
        put(attributes, BiorMetaControlledVocabulary.DATASOURCEPROPERTIES.toString(), datasourcepath);
        put(attributes, BiorMetaControlledVocabulary.COLUMNPROPERTIES.toString(), columnpath);
        List<String> head = h.getMetaData().getOriginalHeader();
        head.add(head.size()-1, buildHeaderLine(attributes));
        return attributes.get(BiorMetaControlledVocabulary.ID.toString()).substring(5); //remove .bior for consistency
    }

    /** Construct a few ##BIOR lines that pertain to a particular catalog, and add it to the metadata header. 
     *  Use for bior_annotate command.
    * @param history       - the history that we need to change
    * @param catalogPath   - path to the catalog file where the data comes from
    * @param operation     - the name of the tool that was called
    * @param newColNamesToAdd - A list of new column names to add (one ##BIOR line for each)
    * @param drilledColNames - The JSON path that was used to get the value that will be placed under the column denoted by newColNamesToAdd
    */
   public void constructAnnotateLine(History history, String catalogPath, String operation, String[] newColNamesToAdd, String[] drilledColNames) throws IOException {
	   boolean isCatalogPathExists = catalogPath != null && catalogPath.trim().length() > 0 && new File(catalogPath).exists();
	   String dataSourcePropsPath = isCatalogPathExists ? getDatasourcePropsPath(catalogPath) 	: null;
	   String columnsPropsPath    = isCatalogPathExists ? getColumnsPropsPath(catalogPath) 		: null;
	   this.constructAnnotateLine(history, dataSourcePropsPath, columnsPropsPath, catalogPath, operation, newColNamesToAdd, drilledColNames);
   }
    
   /** Construct a few ##BIOR lines that pertain to a particular catalog, and add it to the metadata header. 
    *  Use for bior_annotate command.
   * @param history          - the history that we need to change
   * @param dataSourcePath   - path to the datasource properties file for the tool
   * @param dataSourcePath   - path to the datasource properties file for the tool
   * @param pathToUseForPathField - Path to use in the "Path" field in the ##BIOR line. 
   *                                This should be the dataSource path if a tool, else it should be the catalog path.
   * @param operation     - the name of the tool that was called
   * @param newColNamesToAdd - A list of new column names to add (one ##BIOR line for each)
   * @param drilledColNames - The JSON path that was used to get the value that will be placed under the column denoted by newColNamesToAdd
   */
  public void constructAnnotateLine(History history, String dataSourcePath, String columnsDatapath, String pathToUseForPathField, String operation, String[] newColNamesToAdd, String[] drilledColNames) throws IOException {
      LinkedHashMap<String,String> datasourceProps = new LinkedHashMap<String,String>();
      HashMap<String,ColumnMetaData> columnsProps = new HashMap<String,ColumnMetaData>();
	  boolean isDatasourcePropsFileExists = dataSourcePath != null  
   		   &&  dataSourcePath.trim().length() > 0  
   		   &&  new File(dataSourcePath).exists();
      if( isDatasourcePropsFileExists )
    	  datasourceProps = parseDatasourceProperties(dataSourcePath, new LinkedHashMap<String,String>());
      
      boolean isColumnsPropsFileExists = columnsDatapath != null  
   		   &&  columnsDatapath.trim().length() > 0  
   		   &&  new File(columnsDatapath).exists();
      if( isColumnsPropsFileExists )
    	  columnsProps = parseColumnProperties(columnsDatapath);
      
      // There may be multiple columns that were drilled from each catalog
      for(int i=0; i < newColNamesToAdd.length; i++) {
          if(newColNamesToAdd[i] == null)
        	  continue;
        	  
          LinkedHashMap<String,String> attributes = new LinkedHashMap();
          ColumnMetaData cmd = columnsProps.get(drilledColNames[i]);
       
          // Keys that are in every metadata line
          put(attributes, BiorMetaControlledVocabulary.ID.toString(), 			newColNamesToAdd[i]);
          put(attributes, BiorMetaControlledVocabulary.OPERATION.toString(), 	operation);
         
          String datatype = "";
          if(cmd != null && isColumnsPropsFileExists ){
              datatype  =  cmd.getType().toString();
          }
          
          put(attributes, BiorMetaControlledVocabulary.DATATYPE.toString(),datatype);
          // Keys for drilled columns - Add field description if Columns properties file is available, or empty string if it is not
          put(attributes, BiorMetaControlledVocabulary.FIELD.toString(),	drilledColNames[i]);
          // Count for bior_annotate will always be "." because of compress that is called at the end  
          put(attributes, BiorMetaControlledVocabulary.NUMBER.toString(),	".");
          // Delimiter for compress will always be '|' for bior_annotate
          put(attributes, BiorMetaControlledVocabulary.DELIMITER.toString(), "|");
          // Escaped delimiter for compress will always be '\|' for bior_annotate
          put(attributes, BiorMetaControlledVocabulary.ESCAPEDDELIMITER.toString(), "\\|");
          String fieldDesc = "";
          if(cmd != null && isColumnsPropsFileExists )
              fieldDesc  =  cmd.getDescription();
          put(attributes, BiorMetaControlledVocabulary.FIELDDESCRIPTION.toString(), fieldDesc);

          // Keys specific to drilled columns: - add each field if datasource properties file available or empty string if it is not
          String shortName = isDatasourcePropsFileExists  ?  datasourceProps.get(BiorMetaControlledVocabulary.SHORTNAME.toString())  	:  "";
          String source    = isDatasourcePropsFileExists  ?  datasourceProps.get(BiorMetaControlledVocabulary.SOURCE.toString())		:  "";
          String version 	= isDatasourcePropsFileExists  ?  datasourceProps.get(BiorMetaControlledVocabulary.VERSION.toString())		:  "";
          String build 	= isDatasourcePropsFileExists  ?  datasourceProps.get(BiorMetaControlledVocabulary.BUILD.toString())		:  "";
          String desc 		= isDatasourcePropsFileExists  ?  datasourceProps.get(BiorMetaControlledVocabulary.DESCRIPTION.toString())	:  "";
          put(attributes, BiorMetaControlledVocabulary.SHORTNAME.toString(), 	shortName);
          put(attributes, BiorMetaControlledVocabulary.SOURCE.toString(), 		source);
          put(attributes, BiorMetaControlledVocabulary.VERSION.toString(), 		version);
          put(attributes, BiorMetaControlledVocabulary.BUILD.toString(),		build);
          put(attributes, BiorMetaControlledVocabulary.DESCRIPTION.toString(), 	desc);

          // Catalog path
          put(attributes, BiorMetaControlledVocabulary.PATH.toString(),	 		pathToUseForPathField);

          // Build the header line and add it to the header
          String biorHeaderLine = buildHeaderLine(attributes);
          List<String> head = History.getMetaData().getOriginalHeader();
          head.add(head.size()-1, biorHeaderLine);
      }
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
     * ##BIOR=<ID="bior.dbSNP137.INFO.SSR",Operation="DrillPipe",DataType="String",Field="INFO.SSR",FieldDescription="Variant suspect reason code (0 - unspecified, 1 - paralog, 2 - byEST, 3 - Para_EST, 4 - oldAlign, 5 - other)",ShortUniqueName="dbSNP137",Source="dbSNP",Description="dbSNP from NCBI",Version="137",Build="GRCh37.p10",Path="src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz">
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
     * puts type, number and field description into the attributes map
     * @param properties
     * @param attributes
     * @param dpath
     * @return
     */
    public LinkedHashMap<String,String> put3(HashMap<String,ColumnMetaData> properties, LinkedHashMap<String,String> attributes, String dpath){
        if(properties != null){
            String type = properties.get(this.fixArrayDrillPath(dpath)).getType().toString();
            String number = properties.get(this.fixArrayDrillPath(dpath)).getCount();
            String field =  (String) properties.get(this.fixArrayDrillPath(dpath)).getDescription();
            if(type != null && type.length() > 0) put(attributes, BiorMetaControlledVocabulary.DATATYPE.toString(), type);
            if(number != null && number.length() > 0) put(attributes, BiorMetaControlledVocabulary.NUMBER.toString(), number);
            if(field != null && field.length() > 0) put(attributes, BiorMetaControlledVocabulary.FIELDDESCRIPTION.toString(), field);
        }
        return attributes;
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
        put(attributes, BiorMetaControlledVocabulary.ID.toString(),			getID(h,BiorMetaControlledVocabulary.BIOR + datasourceattr.get(BiorMetaControlledVocabulary.SHORTNAME.toString()) + "." + dpath));
        put(attributes, BiorMetaControlledVocabulary.OPERATION.toString(), 	operation);
        put(attributes, BiorMetaControlledVocabulary.FIELD.toString(), 		fixArrayDrillPath(dpath));
        try {
            //attributes = parseDatasourceProperties(datasourceattr.get(BiorMetaControlledVocabulary.PATH.toString()), attributes);
            HashMap<String,ColumnMetaData> properties;
            if(datasourceattr.get(BiorMetaControlledVocabulary.PATH.toString()) != null){
                properties = parseColumnProperties(datasourceattr.get(BiorMetaControlledVocabulary.PATH.toString()));
                attributes = put3(properties, attributes, dpath);
            }else if (datasourceattr.get(BiorMetaControlledVocabulary.COLUMNPROPERTIES.toString()) != null) {
                properties = parseColumnProperties(datasourceattr.get(BiorMetaControlledVocabulary.COLUMNPROPERTIES.toString()));
                attributes = put3(properties, attributes, dpath);
            }else {
                 ; //can't add properties from a properties file
            }

        } catch (IOException e) {
            //else there is not a columns.tsv file, so we can't add a description or datatype
            put(attributes, BiorMetaControlledVocabulary.DATATYPE.toString(), 	ColumnMetaData.Type.String.toString());
            put(attributes, BiorMetaControlledVocabulary.NUMBER.toString(), ".");
            put(attributes, BiorMetaControlledVocabulary.FIELDDESCRIPTION.toString(), "");
        }
        //copy all of the properties from the catalog to the ##BIOR-drill-line
        for( String key : datasourceattr.keySet()){
            if(     key.equalsIgnoreCase(BiorMetaControlledVocabulary.ID.toString()) ||
                    key.equalsIgnoreCase(BiorMetaControlledVocabulary.DATATYPE.toString()) ||
                    key.equalsIgnoreCase(BiorMetaControlledVocabulary.NUMBER.toString()) ||
                    key.equalsIgnoreCase(BiorMetaControlledVocabulary.FIELDDESCRIPTION.toString()) ||
                    key.equalsIgnoreCase(BiorMetaControlledVocabulary.OPERATION.toString()) ||
                    key.equalsIgnoreCase(BiorMetaControlledVocabulary.FIELD.toString()) ||
                    key.equalsIgnoreCase(BiorMetaControlledVocabulary.FIELDDESCRIPTION.toString())
                    ){
                ;   // do nothing
            }else { // add it
                    put(attributes, key, datasourceattr.get(key));
            }
        }
        List<String> head = h.getMetaData().getOriginalHeader();
        head.add(head.size()-1, buildHeaderLine(attributes));
    }

    
    /**
    *
    * @param path the drill path that could contain 1 or many array designations
    *             e.g. path = foo[0].bar.baz[*].x
    * @return   a path without the array desinations for the purpose of looking up the metadata on the leaf node
    *             e.g. path =  foo.bar.baz.x
    */
   public String fixArrayDrillPath(String path){
       if(path.contains("[")){
           String ret = path.replaceAll("\\[[\\*|\\d+]\\]","");
           return ret;
       }else {
           return path;
       }
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
                if(attr.get(BiorMetaControlledVocabulary.ID.toString()).equalsIgnoreCase(headerValue))
                    return i;
            }
            i++;
        }
        return -1;
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
		String[] keys = attributes.keySet().toArray(new String[attributes.size()]);
		for(int i = 0; i < keys.length; i++) {
            String value = attributes.get(keys[i]);
            // Don't add delimiter in front of first value
            String delim = i == 0 ? "" : ",";
            sb.append(delim).append(keys[i]).append("=").append("\"").append(value).append("\"");
		}
		sb.insert(0, "##BIOR=<").append(">");
		return sb.toString();
	}

    /**
     * take a line like:
     * ##BIOR=<ID="bior.dbSNP137",CatalogShortUniqueName="dbSNP137",CatalogSource="dbSNP",CatalogVersion="137",CatalogBuild="GRCh37.p10">
     * and construct a hashmap out of each key=value pair
     * @param line
     * @return
     */
    public LinkedHashMap<String,String> parseHeaderLine(String biorHeaderLine){
        LinkedHashMap<String,String> biorHeaderMap = new LinkedHashMap<String,String>();
        
        biorHeaderLine = biorHeaderLine.trim();
        
        // If the line does not begin with "##BIOR=<" and end with ">", then just return empty map
        if( ! biorHeaderLine.startsWith("##BIOR=<")  && ! biorHeaderLine.endsWith(">") )
        	return biorHeaderMap;
        
        // Remove the "##BIOR=<" off front of string and the ">" at end 
        biorHeaderLine = biorHeaderLine.substring(8, biorHeaderLine.length()-1);
        
        // Split all keys into key=value pairs first (split by comma)
        List<String> keyValuePairs = StringUtils.split(biorHeaderLine, Arrays.asList(","));
        
        // Now split all of those by "=" and add to map
        for(String keyValPair : keyValuePairs) {
        	List<String> keyVal = StringUtils.split(keyValPair, Arrays.asList("="));
        	if( keyVal.size() >= 2 )
        		biorHeaderMap.put(keyVal.get(0), StringUtils.stripOutsideQuotes(keyVal.get(1)));
        }
        
        return biorHeaderMap;
    }
    
}
