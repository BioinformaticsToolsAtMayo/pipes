package edu.mayo.pipes.util.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.mayo.pipes.history.History;
import edu.mayo.pipes.util.PropertiesFileUtil;

public class AddMetadataLines {
	
	public AddMetadataLines() {		
	}

    /**
     * if the operation is drill, then in the header we want to do something like this instead:
     *
     * @param history
     * @param columnName
     * @param catalogFile
     * @return
     */
    public History constructDrillLine(History history, String columnName){
           return history;
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
     * @return
     */
	public History constructMetadataLine(History history, String columnName, String catalogFile) {
		
		String datasourceName = null;
	    PropertiesFileUtil propsUtil = null;
	    String catalogShortUniqueName = null;
	    String catalogSource=null;
	    String catalogVersion=null;
	    String catalogBuild=null;
	    String buildHeaderLine=null;
	    List<String> attributes = null;  
	    List<String> metadataLine = new ArrayList<String>();
		
		if (columnName.contains("bior")) {
			
    		String[] colNameSplit = columnName.split("\\.");
    		
    		if (colNameSplit.length >= 2) {
    			attributes = new ArrayList<String>();
    			
    			datasourceName = colNameSplit[1];
    			//System.out.println("ColumnName="+column_name+"; DatasourceName="+datasourceName);
    			
    			attributes.add("ID=\""+columnName+"\"");
    			
    			try {
    				propsUtil = new PropertiesFileUtil(catalogFile + ".datasource.properties");
				
        			catalogShortUniqueName = propsUtil.get("CatalogShortUniqueName"); 
        	        catalogSource = propsUtil.get("CatalogSource");
        	        catalogVersion = propsUtil.get("CatalogVersion");            	 
        	        catalogBuild = propsUtil.get("CatalogBuild");            	        
        	        
        	        attributes.add("CatalogShortUniqueName=\""+catalogShortUniqueName+"\"");
        	        attributes.add("CatalogSource=\""+catalogSource+"\"");
        	        attributes.add("CatalogVersion=\""+catalogVersion+"\"");
        	        attributes.add("CatalogBuild=\""+catalogBuild+"\"");            	        
        	        
        	        //history.getMetaData().getOriginalHeader().add(buildHeaderLine(attributes));	
        			metadataLine.add(buildHeaderLine(attributes));
        			
        			// Step 4:
        	        System.out.println("ML="+Arrays.asList(metadataLine));
        	        history.getMetaData().getOriginalHeader().add(buildHeaderLine(attributes));
        	        
        	        StringBuilder sb = new StringBuilder();
        	        
        	        List<String> headerLine = history.getMetaData().getOriginalHeader();
        	                	        
        	        for (int i=0;i<headerLine.size()-2; i++) { //do not add the last row, that is the column-header-row
        	        	sb.append(headerLine.get(i));
        	        }
        	        
        	        sb.append(buildHeaderLine(attributes));
        	        
        	        sb.append(history.getMetaData().getColumnHeaderRow("\t"));
        	        
        	        List<String> newHeader = Arrays.asList(sb.toString());
        	        
        	        history.getMetaData().setOriginalHeader(newHeader);
        	        
    			} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
    		} 
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
	public String buildHeaderLine(List<String> attributes) {
		StringBuilder sb = new StringBuilder();
		sb.append("##BIOR=<");
		
		String delim="";
		for(String attrib : attributes) {
			sb.append(delim).append(attrib);
			delim = ",";
		}
		sb.append(">");
		
		return sb.toString();
	}
		
}
