package edu.mayo.pipes.JSON.inject;

import com.google.gson.JsonObject;

import edu.mayo.pipes.history.History;

/**
 * Extracts data from a column and injects it into a JSON object as a JSON Primitive.
 * 
 * NOTE: Columns that are empty or contain "." are not injected.
 *
 */
public class ColumnInjector extends BaseInjector implements Injector, ColumnAware {

	private int      mCol;
	private String   mKey;
	private JsonType mType;

	/**
	 * Constructor
	 * 
	 * NOTE: this will lookup the column header for the specified column and use that as the key
	 * 
	 * @param column Column to extract
	 * @param type JSON primitive type
	 */
	public ColumnInjector(int column, JsonType type) {
		this(column, null, type);
	}
	
	/**
	 * Constructor
	 * 
	 * @param column Column to extract
	 * @param key The name of the JSON Primitive
	 * @param type JSON primitive type
	 */
	public ColumnInjector(int column, String key, JsonType type) {
		if (column == 0) {
			throw new IllegalArgumentException("Zero is not a valid column - columns begin with 1.");
		}
		
		mCol = column;
		mKey = key;
		mType = type;
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
			
			super.injectAsPrimitive(object, key, value, mType);
			
		}
	}

	@Override
	public int getColumn() {
		return mCol;
	}

}
