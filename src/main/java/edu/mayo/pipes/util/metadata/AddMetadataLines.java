package edu.mayo.pipes.util.metadata;

import java.io.IOException;
import java.util.*;

import edu.mayo.pipes.history.History;
import edu.mayo.pipes.util.PropertiesFileUtil;

public class AddMetadataLines {

    /**
     * controled vocabulary for ##BIOR lines
     */
    public static final String OPERATION = "Operation";
    public static final String DATATYPE = "DataType";
    public static final String KEY = "Key";
    public static final String DESCRIPTION = "Description";
    public static final String CATALOGSHORTNAME = "CatalogShortUniqueName";
    public static final String CATALOGSOURCE = "CatalogSource";
    public static final String CATALOGVERSION = "CatalogVersion";
    public static final String CATALOGBUILD = "CatalogBuild";
    public static final String CATALOGPATH = "CatalogPath";
	
	public AddMetadataLines() {		
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

        String catalogShortUniqueName = propsUtil.get(CATALOGSHORTNAME);
        String catalogSource = propsUtil.get(CATALOGSOURCE);
        String catalogVersion = propsUtil.get(CATALOGVERSION);
        String catalogBuild = propsUtil.get(CATALOGBUILD);

        attributes.put(CATALOGSHORTNAME, catalogShortUniqueName);
        attributes.put(CATALOGSOURCE, catalogSource);
        attributes.put(CATALOGVERSION, catalogVersion);
        attributes.put(CATALOGBUILD, catalogBuild);
        attributes.put(CATALOGPATH, catalogPath);

        return attributes;
    }

    public Properties parseColumnProperties(String catalogPath) throws IOException {
        String rootpath = catalogPath;
        if(catalogPath.contains("tsv.bgz")){
            String[] split = catalogPath.split(".tsv.bgz");
            rootpath = split[0];
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
                attributes.put("ID", columnName);
                attributes.put(OPERATION, operation);
                attributes.put(DATATYPE, dataType);
                attributes = this.parseDatasourceProperties(catalogFile, attributes);

                history.getMetaData().getOriginalHeader().add(buildHeaderLine(attributes));

    		} 
    	}
		
		return history; 
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
                        if(phead.get(CATALOGSHORTNAME).equalsIgnoreCase(datasourceName)){
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
            Properties prop = parseColumnProperties(phead.get(CATALOGPATH));
            //for each drill path, append another ##BIOR column to the header

            String description = "";
            description = (String) prop.get(datasourceColumnName);
            if(description == null){
                description = "";
            }

            LinkedHashMap<String,String> attributes = new LinkedHashMap();
            attributes.put("ID", columnName);
            attributes.put(OPERATION, operation);
            attributes.put(DATATYPE, "STRING");
            attributes.put(KEY, datasourceColumnName);
            attributes.put(DESCRIPTION, description);
            attributes = this.parseDatasourceProperties(phead.get(CATALOGPATH), attributes);

            history.getMetaData().getOriginalHeader().add(buildHeaderLine(attributes));

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
        for(String s : split){
            String[] x = s.split("=");
            hm.put(x[0], x[1].replaceAll("\"",""));
        }
        return hm;
    }


		
}
