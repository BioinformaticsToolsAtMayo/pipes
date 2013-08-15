package edu.mayo.pipes.history;


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
	
	public ColumnMetaData(String colName, Type type, String count, String desc) {
		this.columnName = colName;
		this.type = type;
		this.count = count;
		this.description = desc;
	}
	
	public String getColumnName() {
		return columnName;
	}
}
