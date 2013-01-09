package edu.mayo.pipes.JSON;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

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
* 	1		2		100		+		{ "RefAllele":"A", "Chrom":1, "MinBP":2 }
*/
public class InjectIntoJsonPipe  extends AbstractPipe<History, History> {

	private int m_idxJsonCol;
	private SimpleEntry<String,String>[]  m_colIdxAndColNamePairs;
	private History m_colHeaders;
	
	/** 
	 * 
	 * @param indexOfJsonColumn  The column index (1-based) of the JSON column to modify
	 * @param colAndColNamePair  A series of [columnIndex/columnHeader] pairs to pull from and add to the JSON object.
	 *                           NOTE: the column is 1-based
	 *                           This takes 3 types of pairs:
	 *                           <ul>
	 *                             <li>columnIndex with columnHeader of null or "" - this will lookup the column header for the specified column and use that as the key.  Ex:  1 ""</li>
	 *                             <li>columnIndex with columnHeader - this will ignore the column header and use the specified one.  Ex: 1 "Chromosome"</li>
	 *                             <li>columnIndex is a string, in which case this will add a new key/value pair to the JSON object where the key is columnIndex and the value is columnHeader.  Ex: "_type" "Variant" </li>
	 *                           </ul> 
	 */
	public InjectIntoJsonPipe(int indexOfJsonColumn, SimpleEntry<String,String>... colAndColNamePairs) {
		// Throw exception if JSON column index is same as any columnIndex in colAndColNamePair 
		if( isJsonIdxSameAsAnother(indexOfJsonColumn, colAndColNamePairs) )
			throw new IllegalArgumentException("JSON column index cannot be the same as another column index");
		m_idxJsonCol = indexOfJsonColumn;
		m_colIdxAndColNamePairs = colAndColNamePairs;
	}

	@Override
	protected History processNextStart() throws NoSuchElementException {
		History history = this.starts.next();
		History historyOut = (History)history.clone();
		
		// If this is the header line (starts with "#", but NOT "##"), then get all column headers
		if( history.get(0).startsWith("#") ) {
			// Only save the headers if the line starts with a SINGLE #
			if( ! history.get(0).startsWith("##") )
				m_colHeaders = historyOut;
			return historyOut;
		}
		
		// If the JSON column does not exist, then create it
		if(history.size() < m_idxJsonCol) {
			historyOut.add("{}");
			m_colHeaders.add("NewJson");
		}
		
		// Process each line - adding specified columns to the JSON object
		String json = historyOut.get(m_idxJsonCol-1);
		for(SimpleEntry<String,String> colAndColNamePair : m_colIdxAndColNamePairs) {
			SimpleEntry<String,String> keyValPair = getKeyValPairToAdd(colAndColNamePair, history);
			json = addToJson(json, keyValPair);
		}
		historyOut.set(m_idxJsonCol-1, json);
		return historyOut;
	}
	
	//=========================================================================================================
	
	// We should throw an error if the JSON column is the same as a column we want to add
	// (otherwise we will get a recursive add)
	private boolean isJsonIdxSameAsAnother(int indexOfJsonColumn, SimpleEntry<String, String>[] colAndColNamePairs) {
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
		String firstSeparator = isJsonEmpty ? "" : ", ";
		String val = keyValPair.getValue();
		// By default, quote the key and value
		String newJson = json.substring(0,idxLastBrace) + firstSeparator + "\"" + keyValPair.getKey()
				+ "\":\"" + keyValPair.getValue() + "\"" + json.substring(idxLastBrace);
		// If the value is a number, then DON'T quote it
		if( JSONUtil.isInt(val) || JSONUtil.isDouble(val) )
			newJson = json.substring(0,idxLastBrace) + firstSeparator + "\"" + keyValPair.getKey()
			+ "\":" + keyValPair.getValue() + "" + json.substring(idxLastBrace);
		return newJson;
	}

	private SimpleEntry<String,String> getKeyValPairToAdd(SimpleEntry<String,String> colAndColNamePair, History history) {
		SimpleEntry<String,String> keyValPair = null;
		
		// If key is an int, then user specified a column to grab
		if( JSONUtil.isInt(colAndColNamePair.getKey()) ) {
			// Subtract one since column will be 1-based
			int col = Integer.parseInt(colAndColNamePair.getKey()) - 1;
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

	/** col should be 0-based here */
	private String getColumnHeader(int col) {
		String colHeader = "(Unknown)";
		// Since col will be 1-based, subtract one to access array
		// Only assign the column header if it is not null or ""
		if(m_colHeaders != null && col >= 0 && col < m_colHeaders.size() && m_colHeaders.get(col) != null  && m_colHeaders.get(col).length() > 0)
			colHeader = m_colHeaders.get(col);
		// Remove the "#" symbol off front if we are the first header column
		while( colHeader.startsWith("#") )
			colHeader = colHeader.substring(1);
			
		return colHeader;
	}

	
	/** Assumes this is a line that starts with "#" (NOT "##") */
	private List<String> getColumnHeaders(History history) {
		List<String> colHeaders = new ArrayList<String>();
		colHeaders.add(history.get(0).replace("#", "").trim());
		for(int i=1; i < history.size(); i++) {
			colHeaders.add(history.get(i));
		}
		return colHeaders;
	}


}
