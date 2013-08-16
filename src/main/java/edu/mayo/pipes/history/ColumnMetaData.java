package edu.mayo.pipes.history;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.FileExistsException;

/**
 * Describes ColumnName, Type, Count, and Description 
 * that will be used in the ##BIOR metadata header lines,
 * and later in the ##INFO lines when converting the file to VCF format.
 */

public class ColumnMetaData {
	/** The column headers that appear in the catalog.columns.tsv files */
	public enum PropertiesFileHeaders { ColumnName, Type, Count, Description };
	
	public enum Type { JSON, JSONArray, String, Float, Integer, Boolean };
	
	public String columnName;
	public Type   type;
	/** Count is typically an integer to represent the exact number of values that can occur for a key.
	 *  If the type is Boolean, then count should be 0.  If a key always has one value it should be one.
	 *  If the number of values can vary, a dot ('.') should be used */
	public String count;
	public String description;

	
	/**
	 * Most times we only need to work with the column name - if so, this constructor's for you!
	 * @param colName
	 */
	public ColumnMetaData(String colName) {
		columnName = colName;
	}
	
	public ColumnMetaData() { }
	

    public ColumnMetaData(String colName, String type, String count, String desc){
        this(colName, Type.valueOf(type),count,desc);
    }

	public ColumnMetaData(String colName, Type type, String count, String desc) {
		this.columnName = colName;
		this.type = type;
		this.count = count;
		this.description = desc;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	/** Save a list of ColumnMetaData objects to a file, along with the header 
	 * @throws IOException */
	public static void saveMetaData(String columnsTsvPath, List<ColumnMetaData> colMetaList) throws IOException {
		// Add descriptive info at top:
		StringBuilder desc = new StringBuilder();
		desc.append("##-----------------------------------------------------\n");
		desc.append("## Catalog field definitions\n");
		desc.append("##-----------------------------------------------------\n");
		desc.append("## ColumnName=The key or column name\n");
		desc.append("## Type=The type of the object, as can be determined from parsing the VCF file or taking and educated guess based on the catalog values (Possible values: JSON, JSONArray, String, Integer, Float, Boolean)\n"); 
		desc.append("## Count=The number of values that repeatedly occur  (Possible values: 0 (Boolean), 1,2,..,n  or '.' for variable or unknown number of values\n");
		desc.append("## Description=The description of the ColumnName\n");
		desc.append("##-----------------------------------------------------\n");
		desc.append("#ColumnName	Type	Count	Description\n");

		// If the file already exists, throw exception
		File columnsTsvFile = new File(columnsTsvPath);
		if( columnsTsvFile.exists() )
			throw new FileExistsException(columnsTsvFile);

		// Write output to file
		FileOutputStream fout = new FileOutputStream(new File(columnsTsvPath));

		// Write header
		fout.write( (desc.toString() + "\n").getBytes());
		
		// Loop thru all lines and write them
		for(ColumnMetaData colMeta : colMetaList) {
			fout.write( (colMeta.toString() + "\n").getBytes() );
		}
		
		fout.close();
	}
	
	/** Return a tab-delimited string in the format:  ColumnName \t Type \t Count \t Description */
	public String toString() {
		return this.columnName + "\t" + this.type.toString() + "\t" + this.count + "\t" + this.description;
	}
	
	/** Merge two lists of ColumnMetaData objects, and return the combined list.
	 *  NOTE: Items in the left list will take precedence over items in the right list (that have the same name) */
	public static List<ColumnMetaData> merge(List<ColumnMetaData> colMetaListMain, List<ColumnMetaData> colMetaListAdditional) {
		HashSet<String> colNamesAlreadyAdded = new HashSet<String>();
		List<ColumnMetaData> mergedList = new ArrayList<ColumnMetaData>();
		
		// Add all items from the first list
		for(ColumnMetaData colMeta : colMetaListMain) {
			mergedList.add(colMeta);
			colNamesAlreadyAdded.add(colMeta.columnName);
		}
		
		// Now add all items from the right list if they have NOT yet been added.
		for(ColumnMetaData colMeta : colMetaListAdditional) {
			if( ! colNamesAlreadyAdded.contains(colMeta.columnName) ) {
				mergedList.add(colMeta);
				colNamesAlreadyAdded.add(colMeta.columnName);
			}
		}

		return mergedList;
	}
	

    public static HashMap<String,ColumnMetaData> parseColumnProperties(String columnPropertiesPath) throws IOException {
        HashMap<String,ColumnMetaData> descriptions = new HashMap<String,ColumnMetaData>();
        BufferedReader bReader = new BufferedReader(new FileReader(columnPropertiesPath));
        String line;
        while ((line = bReader.readLine()) != null) {
            if(line.startsWith("#")){//it is a comment, chuck it
                ;
            }else {  //contains data
                String[] split = line.split("\t");
                if(split.length > 3){
                    String desc = "";
                    for(int i=3; split.length > i; i++){ //if the description had tabs -- remove them
                        desc = desc + split[i];
                    }
                    ColumnMetaData cd = new ColumnMetaData(split[0], split[1], split[2], desc);
                    descriptions.put(split[0],cd); //key is the fieldName
                }
            }
        }
        return descriptions;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getCount() {
        return count;
    }

    public void setCount(String count) {
        this.count = count;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
