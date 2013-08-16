package edu.mayo.pipes.history;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

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

    public HashMap<String,ColumnMetaData> parseColumnProperties(String columnPropertiesPath) throws IOException {
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
}
