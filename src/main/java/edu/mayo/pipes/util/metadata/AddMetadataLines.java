package edu.mayo.pipes.util.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.mayo.pipes.history.History;
import edu.mayo.pipes.util.PropertiesFileUtil;

public class AddMetadataLines {
	
	public AddMetadataLines() {		
	}

	public History constructMetadataLine(History history, String columnName) {
		
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
    			
    			// TODO
    			//for this datasourcename, find the catalog.datasource.properties file location from the catalogs.properties file
    			String catalogFile = "src/test/resources/testData/metadata/00-All_GRch37.datasource.properties";
    			
    			
    			try {
    				propsUtil = new PropertiesFileUtil(catalogFile);
				
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
	private String buildHeaderLine(List<String> attributes) {
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
