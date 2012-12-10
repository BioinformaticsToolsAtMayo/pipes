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

	/**
	 * Metadata about this history's columns.
	 * 
	 * @return
	 */
	public List<ColumnMetaData> getColumns() {
		return mCols;
	}
}
