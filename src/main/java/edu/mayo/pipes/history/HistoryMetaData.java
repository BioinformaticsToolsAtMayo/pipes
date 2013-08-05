package edu.mayo.pipes.history;

import java.util.ArrayList;
import java.util.List;

/**
 * An object that can be used to get information about the types and properties
 * of the columns in a History object.
 * 
 * @author duffp
 * 
 */
public class HistoryMetaData {

	private List<String> mHeader = new ArrayList<String>();

	private List<ColumnMetaData> mCols = new ArrayList<ColumnMetaData>();

	/**
	 * Constructor
	 * 
	 * @param headerRows
	 *            Rows from original header
	 */
	public HistoryMetaData(List<String> headerRows) {
		mHeader = headerRows;
	}

	/**
	 * Gets the original header unmodified.
	 * 
	 * @return
	 */
	public List<String> getOriginalHeader() {
		return mHeader;
	}
        
        public void setOriginalHeader(List<String> m){
            this.mHeader = m;
        }
	
	/**
	 * Reconstructs column header row dynamically based on ColumnMetaData.
	 * @return
	 */
	public String getColumnHeaderRow(String delimiter) {
		
		List<ColumnMetaData> cols = History.getMetaData().getColumns();

		//  reconstruct column header row dynamically based on meta data
		StringBuilder sb = new StringBuilder();
		
		// only insert # char if 1st column is not "#UNKNOWN"
		if (((cols.size() > 0) && cols.get(0).getColumnName().startsWith("#UNKNOWN")) == false)
		{
			sb.append("#");			
		}
		
		for (int i=0; i < cols.size(); i++) {
			ColumnMetaData cmd = cols.get(i);
			sb.append(cmd.getColumnName().trim());
			
			if (i < (cols.size() - 1)) {
				sb.append(delimiter);
			}					
		}
		return sb.toString();
	}
	
	/**
	 * Metadata about this history's columns.
	 * 
	 * @return
	 */
	public List<ColumnMetaData> getColumns() {
		return mCols;
	}
}
