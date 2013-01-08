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
		//  reconstruct column header row dynamically based on meta data
		StringBuilder sb = new StringBuilder();
		sb.append("#");
		final int numCols = History.getMetaData().getColumns().size();
		for (int i=0; i < numCols; i++) {
			ColumnMetaData cmd = History.getMetaData().getColumns().get(i);
			sb.append(cmd.getColumnName());
			
			if (i < (numCols - 1)) {
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
