package edu.mayo.pipes.JSON;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.JSON.inject.ColumnInjector;
import edu.mayo.pipes.JSON.inject.Injector;
import edu.mayo.pipes.JSON.inject.JsonType;
import edu.mayo.pipes.history.ColumnMetaData;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.util.JSONUtil;

/**
*
* @author Mike Meiners
* Inject specific columns into an existing JSON column
* For example, say you have these columns as input, with JSON as the last column:
* 	Chrom	MinBP	MaxBP	Strand	JSON
* 	1		2		100		+		{ "RefAllele":"A" }
* And say you want to add columns 1 and 2 (Chrom and MinBP) into the JSON object.  The output would be:
* 	Chrom	MinBP	MaxBP	Strand	JSON
* 	1		2		100		+		{ "RefAllele":"A","Chrom":1,"MinBP":2 }
* NOTE: To use the header metadata, this requires this pipe to be added to a Pipeline 
*       and to be preceded by a HistoryInPipe so the HistoryMetadata can be tracked.
*/
public class InjectIntoJsonPipe  extends AbstractPipe<History, History> {

	private int m_idxJsonCol;
	boolean isFirst = true;
	private Injector[] mInjectors;
	public static final String NEW_JSON_HEADER = "bior_injectIntoJson";
	private JsonParser mParser = new JsonParser();
	
	/** 
	 * Constructor
	 * 
	 * @param indexOfJsonColumn  The column index (1-based) of the JSON column to modify
	 * @param injectors	One or more injectors that will inject new content into the designated JSON 
	 */
	public InjectIntoJsonPipe(int indexOfJsonColumn, Injector... injectors) {
		
		// TODO: how to handle this with injectors?
		// Throw exception if JSON column index is same as any columnIndex in colAndColNamePair 
		//if( isJsonIdxSameAsAnother(indexOfJsonColumn, colAndColNamePairs) )
		//	throw new IllegalArgumentException("JSON column index cannot be the same as another column index");

		if( indexOfJsonColumn == 0)
			throw new IllegalArgumentException("Zero is not a valid column - columns begin with 1.");
		
		m_idxJsonCol = indexOfJsonColumn;
		mInjectors = injectors;
	}

	/** 
	 * Constructor
	 * 
	 * @param injectors	One or more injectors that will inject new content into the designated JSON 
	 */
	public InjectIntoJsonPipe(Injector... injectors) {
		m_idxJsonCol = -1;  // Convert this to last column later
		mInjectors = injectors;
	}

	/** 
	 * Constructor 
	 * 
	 * @param columnNames  Users specifies a series of column names for the first x columns, and values are pulled from those columns and assigned a key from the list that the user specified
	 */
	public InjectIntoJsonPipe(String... columnNames) {
		m_idxJsonCol = -1;  // Convert this to last column later
		mInjectors = new Injector[columnNames.length];
		
		for(int i = 0; i < columnNames.length; i++) {
			mInjectors[i] = new ColumnInjector(i+1, columnNames[i], JsonType.STRING);
		}
	}
	
	@Override
	protected History processNextStart() throws NoSuchElementException {
		History history = this.starts.next();
		History historyOut = (History)history.clone();
		
		// Verify that the JSON column index is correct - adjust it if needed
		// Only call this on the first input line
		if(isFirst) {
			isFirst = false;
			adjustJsonColIfNegativeOrNotSet(historyOut);
		}

		// If JSON index is > # of columns, then add a new empty JSON string to end of history
		if(m_idxJsonCol > historyOut.size())
			historyOut.add("{}");
		
		// Process each line - adding specified columns to the JSON object
		String json = historyOut.get(m_idxJsonCol-1);
		JsonObject object = mParser.parse(json).getAsJsonObject();
		
		for(Injector injector: mInjectors) {
			injector.inject(object, history);
		}
		
		historyOut.set(m_idxJsonCol-1, object.toString());
		return historyOut;
	}
	

	//=========================================================================================================
	
	/** Adjust the JSON column index as needed.   This will be called on every new input line, but the JSON index should only be set once */
	private void adjustJsonColIfNegativeOrNotSet(History histOut) {
		// If JSON column does not exist, then create it and add to the metadata
		if(m_idxJsonCol > histOut.size())
			addNewJsonColumnHeader();
		else if(m_idxJsonCol == 0 )
			throw new NoSuchElementException("JSON column cannot be zero");
		// If idx is negative, then wrap around to end of the line
		else if(m_idxJsonCol < 0) {
			m_idxJsonCol = histOut.size() + (m_idxJsonCol+1);
			
			// TODO: HELP! talk to mike about this
			
			// if the last col does not contain json, OR
			// if it does but the new json index is <= the largest col # in the keyValPairs,
			// THEN create a new json column at the end
//			boolean isLastColJson = isJsonInColumn(histOut.get(m_idxJsonCol-1));
//			if( ! isLastColJson  || (isLastColJson && m_idxJsonCol <= getHighestKeyColumn()) ) {
//				m_idxJsonCol = histOut.size() + 1;
//				addNewJsonColumnHeader();
//			}
		}
		// If the target json column does not actually contain json, 
		// then we will add a new JSON column to the end
		else if( ! isJsonInColumn(histOut.get(m_idxJsonCol-1)) ) {
			m_idxJsonCol = histOut.size() + 1;
			addNewJsonColumnHeader();
		}
	}

	private boolean isJsonInColumn(String possibleJson) {
		return possibleJson.startsWith("{")  &&  possibleJson.endsWith("}");
	}
	
	/** Only add the next header to the HistoryMetaData if the HistoryMetaData exists.
	 *  This should only be called ONCE! */
	private void addNewJsonColumnHeader() {
		List<ColumnMetaData> headers = History.getMetaData().getColumns();
		if( headers != null && headers.size() > 0 )
			headers.add(new ColumnMetaData(NEW_JSON_HEADER));
	}
	
	// We should throw an error if the JSON column is the same as a column we want to add
	// (otherwise we will get a recursive add into the JSON target column)
	private boolean isJsonIdxSameAsAnother(int indexOfJsonColumn, SimpleEntry[] colAndColNamePairs) {
		for(SimpleEntry<String,String> keyValPair : colAndColNamePairs) {
			if( Integer.toString(indexOfJsonColumn).equals(keyValPair.getKey()) )
				return true;
		}
		return false;
	}


	private String addToJson(String json, SimpleEntry<String,String> keyValPair) {
		String val = keyValPair.getValue();

		// If value is null, empty, or ".", then just return original json (don't add to JSON)
		if(val == null || val.length() == 0 || val.equals("."))
			return json;
		
		int idxOpenBrace = json.indexOf("{");
		int idxLastBrace = json.lastIndexOf("}");
		boolean isJsonEmpty = json.substring(idxOpenBrace, idxLastBrace).trim().length() == 1;
		String firstSeparator = isJsonEmpty ? "" : ",";
		// By default, quote the key and value
		String newJson = json.substring(0,idxLastBrace) + firstSeparator + "\"" + keyValPair.getKey()
				+ "\":\"" + val + "\"" + json.substring(idxLastBrace);
		// If the value is a number, OR a JSON substring, then DON'T quote it
		if( JSONUtil.isInt(val) || JSONUtil.isDouble(val) || isJsonInColumn(val) )
			newJson = json.substring(0,idxLastBrace) + firstSeparator + "\"" + keyValPair.getKey()
				+ "\":" + val + "" + json.substring(idxLastBrace);
			
		return newJson;
	}

	private SimpleEntry<String,String> getKeyValPairToAdd(SimpleEntry colAndColNamePair, History history) {
		SimpleEntry<String,String> keyValPair = null;
		
		// If key is an int, then user specified a column to grab
		if( JSONUtil.isInt("" + colAndColNamePair.getKey()) ) {
			// Subtract one since column will be 1-based
			int col = Integer.parseInt("" + colAndColNamePair.getKey()) - 1;
			// If the column header value is null or "", then use the header from the header row as key
			String key = "";
			if( null == colAndColNamePair.getValue() || "".equals(colAndColNamePair.getValue()) )
				key = getColumnHeader(col);
			// Else, user specified the header, so use that
			else
				key = (String)(colAndColNamePair.getValue());
			String val = history.get(col);
			keyValPair = new SimpleEntry<String,String>(key, val);
		}
		// Else, the user wants to specify a key AND value pair to add directly to the JSON (no column lookup)
		else {
			String key = (String)(colAndColNamePair.getKey());
			String val = (String)(colAndColNamePair.getValue());
			keyValPair = new SimpleEntry<String,String>(key, val);
		}
		return keyValPair;
	}

	/** col should be 0-based when passed in here  */
	private String getColumnHeader(int col) {
		// Since col will be 1-based, subtract one to access array
		// Only assign the column header if it is not null or ""
		if( History.getMetaData().getOriginalHeader().size() == 0 )
			return "(Unknown)";
		else
			return History.getMetaData().getColumns().get(col).getColumnName();
	}	
}
