package edu.mayo.pipes.history;

public class ColumnMetaData {
	private String mColName;
	
	/**
	 * Constructor
	 * 
	 * @param colName
	 */
	public ColumnMetaData(String colName) {
		mColName = colName;
	}
	
	public String getColumnName() {
		return mColName;
	}
}
