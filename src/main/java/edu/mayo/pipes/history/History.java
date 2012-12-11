package edu.mayo.pipes.history;

import java.util.ArrayList;
import java.util.List;

/**
 * A list of String values representing a single row of tablular data.
 * 
 * @author duffp
 * 
 */
public class History extends ArrayList<String> implements List<String>,
		Cloneable {

	private static final long serialVersionUID = 1L;

	// declared as a class variable since it will be created ONCE and used by
	// multiple instances of the History class
	private static HistoryMetaData sMetaData = null;

	private static boolean sMetaDataInitialized = false;
	
	protected void setMetaData(HistoryMetaData hMetaData) {
		sMetaData = hMetaData;
		sMetaDataInitialized = true;
	}
	
	protected boolean isMetaDataInitialized() {
		return sMetaDataInitialized;
	}
	
	/**
	 * Retrieves the number, types and properties of this History's columns.
	 * 
	 * @return the description of this History's columns
	 */
	public static HistoryMetaData getMetaData() {
		return sMetaData;
	}

	/**
	 * Merges the list of String values into a single String.
	 * 
	 * @param delimiter
	 *            String that delimits each column of data
	 * @return the merged string. If this History has 0 values, an empty String
	 *         is returned.
	 */
	public String getMergedData(String delimiter) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < size(); i++) {
			String dataStr = get(i);

			sb.append(dataStr);

			if (i < (size() - 1)) {
				sb.append(delimiter);
			}
		}
		return sb.toString();
	}
}