package edu.mayo.pipes.history;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.mayo.pipes.util.StringUtils;

/**
 * A list of String values representing a single row of tablular data.
 * 
 * @author duffp
 * 
 */
public class History extends ArrayList<String> implements List<String>,	Cloneable {
    
	private static final String COL_DELIMITER = "\t";

	
	@Override
	/** Had to override the clone method because the default ArrayList<String> clone was
	 *  not correctly copying JSON arrays (ex: "A":["x","y"]) 
	 *  NOTE:  Collections.copy() does NOT work either in this case!  */
	public Object clone() {
		History h2 = new History();
		for(int i=0; i < this.size(); i++) {
			h2.add(new String(this.get(i)));
		}
		return h2;
	}

    public History(){
    }
    
    /** Create a History object from a line that is tab-delimited */
    public History(String lineTabDelimited) {
		// split data row, add to history
		// SAFE split is required because there may be empty fields between delimiters
		this(StringUtils.safeSplit(lineTabDelimited, COL_DELIMITER));
    }
    
    /** Create a History object from a line that is broken into columns */
    public History(List<String> line) {
    	this(line.toArray(new String[line.size()]));
    }

    
    /** Create a History object from a line that is broken into columns */
    public History(String[] line) {
		for (String colData : line) {
			add(colData);
		}
    }
    
    
    /** Remove all header metadata information */
    public static void clearMetaData() {
    	sMetaData = null;
    	sMetaDataInitialized = false;
    }

    
	private static final long serialVersionUID = 1L;

	// declared as a class variable since it will be created ONCE and used by
	// multiple instances of the History class
	private static HistoryMetaData sMetaData = null;

	private static boolean sMetaDataInitialized = false;
	
	public void setMetaData(HistoryMetaData hMetaData) {
		sMetaData = hMetaData;
		sMetaDataInitialized = true;
	}
	
	public boolean isMetaDataInitialized() {
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
