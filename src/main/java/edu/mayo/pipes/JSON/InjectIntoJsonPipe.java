package edu.mayo.pipes.JSON;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.history.ColumnMetaData;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryMetaData;
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
	private SimpleEntry[]  m_colIdxAndColNamePairs;
	boolean isFirst = true;
	public static final String NEW_JSON_HEADER = "bior_injectIntoJson";
	
	/** 
	 * 
	 * @param indexOfJsonColumn  The column index (1-based) of the JSON column to modify
	 * @param colAndColNamePair  A series of [columnIndex/columnHeader] pairs to pull from and add to the JSON object.
	 *                           NOTE: the column is 1-based
	 *                           This takes 3 types of pairs:
	 *                           <ul>
	 *                             <li>Ex: 1 null -- columnIndex with columnHeader of null or "" - this will lookup the column header for the specified column and use that as the key.  </li>
	 *                             <li>Ex: 1 "Chromosome" -- columnIndex with columnHeader - this will ignore the column header and use the specified one.  </li>
	 *                             <li>Ex: "_type" "Variant" -- columnIndex is a string, in which case this will add a new key/value pair to the JSON object where the key is columnIndex and the value is columnHeader.   </li>
	 *                           </ul> 
	 */
	public InjectIntoJsonPipe(int indexOfJsonColumn, SimpleEntry... colAndColNamePairs) {
		// Throw exception if JSON column index is same as any columnIndex in colAndColNamePair 
		if( isJsonIdxSameAsAnother(indexOfJsonColumn, colAndColNamePairs) )
			throw new IllegalArgumentException("JSON column index cannot be the same as another column index");
		if( isAnyColumnZero(indexOfJsonColumn, colAndColNamePairs) )
			throw new IllegalArgumentException("Zero is not a valid column - columns begin with 1.");
		m_idxJsonCol = indexOfJsonColumn;
		m_colIdxAndColNamePairs = colAndColNamePairs;
	}

	@Override
	protected History processNextStart() throws NoSuchElementException {
		History history = this.starts.next();
		History historyOut = (History)history.clone();
		
		// If the JSON column does not exist, then create it and add to the metadata
		if(history.size() < m_idxJsonCol) {
			historyOut.add("{}");
			addNewJsonColumnHeader();
		}
		
		// Process each line - adding specified columns to the JSON object
		String json = historyOut.get(m_idxJsonCol-1);
		for(SimpleEntry colAndColNamePair : m_colIdxAndColNamePairs) {
			SimpleEntry<String,String> keyValPair = getKeyValPairToAdd(colAndColNamePair, history);
			json = addToJson(json, keyValPair);
		}
		historyOut.set(m_idxJsonCol-1, json);
		return historyOut;
	}
	

	//=========================================================================================================
	
	private boolean isAnyColumnZero(int indexOfJsonColumn, SimpleEntry[] colAndColNamePairs) {
		if( indexOfJsonColumn == 0 )
			return true;
		for(SimpleEntry keyValPair : colAndColNamePairs) {
			if( "0".equals(keyValPair.getKey()) )
				return true;
		}
		return false;
	}
	
	/** Only add the next header to the HistoryMetaData if the HistoryMetaData exists */
	private void addNewJsonColumnHeader() {
		if(isFirst) {
			isFirst = false;
			List<ColumnMetaData> headers = History.getMetaData().getColumns();
			if( headers != null && headers.size() > 0 )
				headers.add(new ColumnMetaData(NEW_JSON_HEADER));
		}
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
		int idxOpenBrace = json.indexOf("{");
		int idxLastBrace = json.lastIndexOf("}");
		boolean isJsonEmpty = json.substring(idxOpenBrace, idxLastBrace).trim().length() == 1;
		String firstSeparator = isJsonEmpty ? "" : ",";
		String val = keyValPair.getValue();
		// By default, quote the key and value
		String newJson = json.substring(0,idxLastBrace) + firstSeparator + "\"" + keyValPair.getKey()
				+ "\":\"" + val + "\"" + json.substring(idxLastBrace);
		// If the value is a number, then DON'T quote it
		if( JSONUtil.isInt(val) || JSONUtil.isDouble(val) )
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
			if( null == colAndColNamePair.getValue() || "".equals(colAndColNamePair.getValue()) ) {
				String key = getColumnHeader(col);
				String val = history.get(col);
				keyValPair = new SimpleEntry<String,String>(key, val);
			}
			// Else, user specified the header, so use that
			else {
				String key = (String)(colAndColNamePair.getValue());
				String val = history.get(col);
				keyValPair = new SimpleEntry<String,String>(key, val);
			}
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
