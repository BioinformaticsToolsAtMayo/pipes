package edu.mayo.pipes.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
/*
 * Utility class that reads the metadata from Catalog properties file.
 */
public class MetadataProperties {
	
	Properties prop = new Properties();
	
  public Map<String,String>	getDataSourceMetadata(String propertiesfile) {
		Map<String,String>  meta = new HashMap<String,String>();
        File f = new File(propertiesfile);
        
        if (f.exists()){
        	
        	try {
        	
        		prop.load(new FileInputStream(f));
        		meta.put("CatalogShortUniqueName",prop.getProperty("CatalogShortUniqueName"));
        		meta.put("CatalogDescription",prop.getProperty("CatalogDescription"));
        		meta.put("CatalogSource", prop.getProperty("CatalogSource"));
        		meta.put("CatalogVersion", prop.getProperty("CatalogVersion"));
        		meta.put("CatalogBuild", prop.getProperty("CatalogBuild"));
        	
        	} catch (IOException ex) {
        		
        	
        	}
        	
        } 
		
		return meta;
	}
  
  public String	getDataSourceColumnMetadata(String propertiesfile,String column) {
		String  meta = null;
      File f = new File(propertiesfile);
      
      if (f.exists()){
      	
      	try {
      	
      		prop.load(new FileInputStream(f));
      		meta = prop.getProperty(column);
      	
      	} catch (IOException ex) {
      		
      	
      	}
      	
      } 
		
		return meta;
	}

}
