package edu.mayo.pipes.JSON.inject;

import com.google.gson.JsonObject;

import edu.mayo.pipes.history.History;

/**
 * Extracts data from a column and injects it into a JSON object as a JSON Array.
 * 
 * NOTE: Columns that are empty or contain "." are injected as a JSON Array with 0 items.
 *
 */
public class ColumnArrayInjector extends BaseInjector implements Injector, ColumnAware {

	private int      mCol;
	private String   mKey;
	private JsonType mType;
	private String   mDelimiterRegex;
	
	/**
	 * Constructor
	 * 
	 * NOTE: this will lookup the column header for the specified column and use that as the key
	 * 
	 * @param column Column to extract the array data from
	 * @param type JSON primitive type to be used in JSON Array values
	 * @param delimiterRegex Delimiter Regular Expression used to split column array values
	 */
	public ColumnArrayInjector(int column, JsonType type, String delimiterRegex) {
		this(column, null, type, delimiterRegex);
	}
	
        /**
	 * Constructor
	 * 
	 * @param column Column to extract the array data from
	 * @param key The name of the JSON Array
	 * @param type JSON primitive type to be used in JSON Array values
	 * @param delimiterRegex Delimiter Regular Expression used to split column array values
         * @param delimiterRegex strip the whitespace out of the injected values before injecting them.
	 */
        private boolean strip = false;
        public ColumnArrayInjector(int column, String key, JsonType type, String delimiterRegex, boolean stripWhitespace) {          
            init( column,  key,  type,  delimiterRegex);
            this.strip = stripWhitespace;
        }
        
        
	/**
	 * Constructor
	 * 
	 * @param column Column to extract the array data from
	 * @param key The name of the JSON Array
	 * @param type JSON primitive type to be used in JSON Array values
	 * @param delimiterRegex Delimiter Regular Expression used to split column array values
	 */
	public ColumnArrayInjector(int column, String key, JsonType type, String delimiterRegex) {
            init( column,  key,  type,  delimiterRegex);
        }
        
        private void init(int column, String key, JsonType type, String delimiterRegex){
		if (column == 0) {
			throw new IllegalArgumentException("Zero is not a valid column - columns begin with 1.");
		}
		
		mCol = column;
		mKey = key;
		mType = type;
		mDelimiterRegex = delimiterRegex;
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
		

			String[] values;
			if((value.trim().length() == 0) || value.trim().equals(".")) {
				// there are zero values iin this case
				values = new String[0];
			}
			else {
				values = value.split(mDelimiterRegex);
			}
                        
                        if(strip == true){
                            for(int i=0; i< values.length; i++){
                                values[i] = values[i].trim();
                            }
                        }
                        
			super.injectAsArray(object, key, values, mType);
			
	}

	@Override
	public int getColumn() {
		return mCol;
	}

}
