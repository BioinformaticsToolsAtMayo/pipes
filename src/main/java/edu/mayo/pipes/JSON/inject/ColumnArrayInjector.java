package edu.mayo.pipes.JSON.inject;

import com.google.gson.JsonObject;

import edu.mayo.pipes.history.History;

/**
 * Extracts data from a column and injects it into a JSON object as a JSON Array.
 * 
 * NOTE: Columns that are empty or contain "." are not injected.
 *
 */
public class ColumnArrayInjector extends BaseInjector implements Injector {

	private int      mCol;
	private String   mKey;
	private JsonType mType;
	private String   mDelimiter;
	
	/**
	 * Constructor
	 * 
	 * NOTE: this will lookup the column header for the specified column and use that as the key
	 * 
	 * @param column Column to extract the array data from
	 * @param type JSON primitive type to be used in JSON Array values
	 * @param delimiter Delimiter used inside the column to delimit array values
	 */
	public ColumnArrayInjector(int column, JsonType type, String delimiter) {
		this(column, null, type, delimiter);
	}
	
	/**
	 * Constructor
	 * 
	 * @param column Column to extract the array data from
	 * @param key The name of the JSON Array
	 * @param type JSON primitive type to be used in JSON Array values
	 * @param delimiter Delimiter used inside the column to delimit array values
	 */
	public ColumnArrayInjector(int column, String key, JsonType type, String delimiter) {
		if (column == 0) {
			throw new IllegalArgumentException("Zero is not a valid column - columns begin with 1.");
		}
		
		mCol = column;
		mKey = key;
		mType = type;
		mDelimiter = delimiter;
	}
	
	@Override
	public void inject(JsonObject object, History history) {
		
		String key;
		if (mKey == null) {
			key = history.getMetaData().getColumns().get(mCol - 1).getColumnName();
		} else {
			key = mKey;
		}
		
		String value = history.get(mCol - 1);
		
		if((value.length() > 0) && !value.equals(".")) {
			
			String[] values = value.split(mDelimiter);
			super.injectAsArray(object, key, values, mType);
			
		}
	}

}
