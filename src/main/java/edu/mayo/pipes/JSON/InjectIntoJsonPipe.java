package edu.mayo.pipes.JSON;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.JSON.inject.ColumnAware;
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

	private int mIdxJsonCol;
	boolean mIsFirst = true;
	private Injector[] mInjectors;
	public static final String NEW_JSON_HEADER = "bior_injectIntoJson";
	private JsonParser mParser = new JsonParser();
	private boolean mIsCreateNewJsonColumn = false;
	
	/** 
	 * Constructor - User explicitly specifies the JSON column to inject into 
	 * @param indexOfJsonColumn  The column index (1-based) of the JSON column to modify
	 * @param injectors	One or more injectors that will inject new content into the designated JSON 
	 */
	public InjectIntoJsonPipe(int indexOfJsonColumn, Injector... injectors) {
		// Throw exception if JSON column index is same as any columnIndex in colAndColNamePair 
		if( isJsonIdxSameAsAnother(indexOfJsonColumn, injectors) )
			throw new IllegalArgumentException("JSON column index cannot be the same as another column index");

		if( indexOfJsonColumn == 0)
			throw new IllegalArgumentException("Zero is not a valid column - columns begin with 1.");
		
		mIdxJsonCol = indexOfJsonColumn;
		mInjectors = injectors;
	}

	/** 
	 * Constructor - Allow user to create a new JSON column, or let it use the last column for JSON
	 * @param injectors	One or more injectors that will inject new content into the designated JSON 
	 * @param isCreateNewJsonColumn  If true, create new json column on end, else use last column.
	 */
	public InjectIntoJsonPipe(boolean isCreateNewJsonColumn, Injector... injectors) {
		mIdxJsonCol = -1;  // Convert this to last column later
		mIsCreateNewJsonColumn = isCreateNewJsonColumn;
		mInjectors = injectors;
	}

	/** 
	 * Constructor - User injects data into the last column, which must be JSON
	 * @param injectors	One or more injectors that will inject new content into the designated JSON 
	 * @param isCreateNewJsonColumn  If true, create new json column on end, else use last column.
	 */
	public InjectIntoJsonPipe(Injector... injectors) {
		this(false, injectors);
	}

	/** 
	 * Constructor - User chooses multiple STRING columns to inject into the last column which must be JSON
	 * @param columnNames	Key names to use for the first x columns that will be injected into the last column 
	 */
	public InjectIntoJsonPipe(String... columnNames) {
		this(false, createStringColumnInjectors(columnNames));
	}
	
	/** 
	 * Constructor - User chooses multiple STRING columns to inject into a new JSON column
	 * @param columnNames	Key names to use for the first x columns that will be injected into the last column 
	 * @param isCreateNewJsonColumn  If true, create new json column on end, else use last column.
	 */
	public InjectIntoJsonPipe(boolean isCreateNewJsonColumn, String... columnNames) {
		this(isCreateNewJsonColumn, createStringColumnInjectors(columnNames));
	}

	@Override
	protected History processNextStart() throws NoSuchElementException {
		if(! this.starts.hasNext())
			throw new NoSuchElementException();
		
		History history = this.starts.next();
		History historyOut = (History)history.clone();
		
		// If we need to create a new JSON column, OR JSON index is > # of columns, 
		// then add a new empty JSON string to end of history
		if(mIsCreateNewJsonColumn || (mIdxJsonCol > historyOut.size()) ) {
			mIdxJsonCol = historyOut.size() + 1;
			historyOut.add("{}");
			if( mIsFirst ) {
				addNewJsonColumnHeader();
				mIsFirst = false;
			}
		}
		else if(mIdxJsonCol == -1) {
			mIdxJsonCol = historyOut.size();
		}
		
		// Process each line - adding specified columns to the JSON object
		String json = historyOut.get(mIdxJsonCol-1);
		if( ! isAJsonColumn(json) )
			throw new IllegalArgumentException("The JSON column to inject into does not contain a JSON object or valid JSON");
		JsonObject object = mParser.parse(json).getAsJsonObject();
		
		for(Injector injector: mInjectors) {
			injector.inject(object, history);
		}
		
		historyOut.set(mIdxJsonCol-1, object.toString());
		return historyOut;
	}
	

	//=========================================================================================================

	private boolean isAJsonColumn(String json) {
		return json.startsWith("{")  &&  json.endsWith("}");
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
	private boolean isJsonIdxSameAsAnother(int indexOfJsonColumn, Injector[] injectors) {
		for(Injector injector : injectors) {
			if( (injector instanceof ColumnAware) && (indexOfJsonColumn == ((ColumnAware)injector).getColumn()) )
				return true;
		}
		return false;
	}
	
	private static Injector[] createStringColumnInjectors(String[] columnNames) {
		ColumnInjector[] injectors = new ColumnInjector[columnNames.length];
		for(int i=0; i < injectors.length; i++) {
			injectors[i] = new ColumnInjector(i+1, columnNames[i], JsonType.STRING);
		}
		return injectors;
	}
}
