package edu.mayo.pipes.history;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.util.FieldSpecification;
import edu.mayo.pipes.util.FieldSpecification.FieldType;

/**
 * Compresses multiple "similar" rows into 1 row.  Rows are defined to be "similar"
 * if all column values are identical except for columns specified to be compressed.
 * 
 * @author duffp
 *
 */
public class CompressPipe extends AbstractPipe<History, History>
{
	
	private String mDelimiter;
	private String mEscDelimiter;
	
	private FieldSpecification mFieldSpec;
			
	private List<List<String>> mBuffer    = new ArrayList<List<String>>();
	private List<String>       mBufferKey = null;

	private boolean mFieldsInitialized = false;

	// NOTE: Since fields are 1-based, accessing the corresponding field value
	// in a list is done via:
	//
	// <code>  myList.get(field - 1);  </code>
	//
	private List<Integer> mKeyFields      = new ArrayList<Integer>();
	private List<Integer> mCompressFields = new ArrayList<Integer>();
	
	// flag indicating previous pipe has no more data
	private boolean mNoMoreData = false;
	
	// flag indicated whether values will be compressed based on "SET" logic
	private boolean mSetCompression;
	
	/**
	 * Constructor
	 *
	 * @param fieldSpec
	 * 		{@link FieldSpecification} that specifies which fields will be compressed. <p/>
	 * @param delimiter
	 * 		Delimiter used to concat multiple row values for 1 column into a single cel value.
	 * 		If the field contains the specified <b>delimiter</b>, it will be escaped by
	 * 		prefixing a blackslash "\" character.
	 */
	public CompressPipe(FieldSpecification fieldSpec, String delimiter)
	{
		this(fieldSpec, delimiter, "\\" + delimiter, false);
	}

	/**
	 * Constructor
	 *
	 * @param fieldSpec
	 * 		{@link FieldSpecification} that specifies which fields will be compressed. <p/>
	 * @param delimiter
	 * 		Delimiter used to concat multiple row values for 1 column into a single cel value.
	 * @param escapeDelimiter
	 * 		If a given field contains the specified <b>delimiter</b> already, the value of this
	 * 		parameter will be used to escape it.
	 * @param useSetCompression
	 * 		"SET" compression will keep only the <b>unique</b> row values rather than all
	 * 		values.  The order of the row values is preserved.
	 */
	public CompressPipe(FieldSpecification fieldSpec, String delimiter, String escapeDelimiter, boolean useSetCompression)
	{
		mFieldSpec = fieldSpec;
		mDelimiter = delimiter;
		mEscDelimiter = escapeDelimiter;
		mSetCompression = useSetCompression;
	}	
	
	@Override
	protected History processNextStart() throws NoSuchElementException
	{
		// signal this pipe is done if previous pipe has no more data
		if (mNoMoreData)
			throw new NoSuchElementException();

		
		// loop until a batch of input lines are compressed to a single line
		while (true)
		{
			List<String> line;
			try
			{
				line = this.starts.next();
			}
			catch (NoSuchElementException e)
			{								
				// no more data from previous pipe
				// compress what has accumulated in the buffer 
				mNoMoreData = true;
				return compress(mBuffer);
			}
			
			// one-time initialization of fields
			if (mFieldsInitialized == false)
			{
				initializeFields(line.size());
				mFieldsInitialized = true;
			}
			
			final List<String> lineKey = getKey(line);

			// if empty buffer or matching keys, add to buffer
			if ((mBuffer.size() == 0) || hasKeyMatch(mBufferKey, lineKey))
			{
				// set key if buffer is empty
				if (mBuffer.size() == 0)
					mBufferKey = lineKey;
				
				mBuffer.add(line);
			}
			// otherwise key mismatch 
			else
			{
				History compressedLine = compress(mBuffer);

				// flush buffer
				mBuffer.clear();
				mBufferKey = null;				
				
				// add mismatch to buffer
				mBufferKey = lineKey;
				mBuffer.add(line);
				
				return compressedLine;			
			}
		}		
	}

	/**
	 * Initializes the key and compress field numbers.
	 *  
	 * @param numFields
	 * 		total number of fields in the input {@link java.util.List}.
	 */
	private void initializeFields(int numFields)
	{
		Map<FieldType, List<Integer>> m = mFieldSpec.getFields(numFields);
		
		mCompressFields = m.get(FieldType.MATCH);
		mKeyFields = m.get(FieldType.NON_MATCH);		
	}
	
	/**
	 * Compresses the given lines where applicable.  Results are dumped into
	 * a new History object.
	 * 
	 * @param lines
	 * @return
	 */
	private History compress(List<List<String>> lines)
	{
		// check if there's nothing to compress
		if (lines.size() == 0)
		{
			return new History();
		}

		final int numCols = lines.get(0).size();

		final boolean[] identicalColVals = checkSameColumnValues(lines);		

		// 1 row, each column contains list of Strings that represent values
		List<List<String>> singleRow = new ArrayList<List<String>>();
		for (int col=0; col < numCols; col++)
		{
			singleRow.add(new ArrayList<String>());
		}
		
		// count of individual values that are period chars '.' per column
		int[] periodCnt = new int[numCols];
		
		// for each line
		for (List<String> line: lines)
		{
			// for each column
			for (int col=0; col < numCols; col++)
			{				
				// fields are 1-based
				int field = col + 1;
				
				// if the column is new OR supposed to be compressed
				if ((singleRow.get(col).size() == 0) || mCompressFields.contains(field))
				{
					String colValue = line.get(col);
					
					if (!mSetCompression || !singleRow.get(col).contains(colValue))
					{
						if (colValue.equals("."))
						{
							periodCnt[col]++;
						}
						singleRow.get(col).add(colValue);
					}
				}
			}			
		}
		
		// list of StringBuilders, 1 per column for final compressed line
		List<StringBuilder> builders = new ArrayList<StringBuilder>();
		for (int col=0; col < numCols; col++)
		{
			builders.add(new StringBuilder());
		}
		
		// for each column, build compressed Strings
		for (int col=0; col < numCols; col++)
		{			
			// fields are 1-based
			int field = col + 1;

			StringBuilder builder = builders.get(col);				

			if ((identicalColVals[col] == false) && mCompressFields.contains(field))
			{				
				boolean ignorePeriods = false;
				if (mSetCompression && (periodCnt[col] > 0) && (periodCnt[col] < singleRow.get(col).size()))
				{
					ignorePeriods = true;
				}
				
				for (String value: singleRow.get(col))
				{
					if (ignorePeriods && (value.equals(".")))
					{
						continue;
					}
					
					// escape occurrences of delimiter
					value = value.replace(mDelimiter, mEscDelimiter);
					
					builder.append(value);
					builder.append(mDelimiter);
				}
				
				// chomp trailing delimiter
				builder.deleteCharAt(builder.length() - 1);
			}
			else
			{
				// otherwise, just add 1st value only
				String value = singleRow.get(col).get(0);
				
				// escape occurrences of delimiter
				value = value.replace(mDelimiter, mEscDelimiter);

				builder.append(value);
			}			
		}
		
		// translate StringBuilder to History
		History compressedLine = new History();
		for (StringBuilder builder: builders)
		{
			compressedLine.add(builder.toString());
		}
		
		return compressedLine;
	}

	/**
	 * Checks all values for each column.  If all values of a specific column
	 * are identical, the corresponding element in the boolean[] is TRUE, FALSE
	 * otherwise.
	 * 
	 * @param lines
	 * @return
	 * 		Array of booleans that correspond 1-to-1 with the compress columns.
	 * 		Boolean is TRUE if all values in column are identical.  FALSE otherwise.
	 */
	private boolean[] checkSameColumnValues(List<List<String>> lines)
	{
		if (lines.size() == 0)
		{
			return new boolean[0];
		}
		
		final int numCols = lines.get(0).size();		
		
		// for-each column
		boolean[] identicalColVals = new boolean[numCols];
		for (int col=0; col < numCols; col++)
		{
			boolean allIdentical = true;
			
			String colValue = null;
			for (List<String> line: lines)
			{
				if (colValue == null)
				{
					colValue = line.get(col);
				}
				else if (colValue.equals(line.get(col)) == false)
				{
					allIdentical = false;
					break;
				}
			}
			
			identicalColVals[col] = allIdentical;
		}
		
		return identicalColVals;
	}
	
	/**
	 * Determines whether the 2 specified keys are a match.
	 * 
	 * @param key1
	 * @param key2
	 * @return
	 */
	private boolean hasKeyMatch(List<String> key1, List<String> key2)
	{		
		boolean isMatch = true;
		for (int i=0; i < key1.size(); i++)
		{
			String keyValue1 = key1.get(i);
			String keyValue2 = key2.get(i);
			if (!keyValue1.equals(keyValue2))
			{
				isMatch = false;
				break;
			}
		}
		return isMatch;
	}
	
	/**
	 * Gets the key for the specified line.
	 * @param line
	 * @return
	 */
	private List<String> getKey(List<String> line)
	{
		List<String> key = new ArrayList<String>();
		for (int index: mKeyFields)
		{
			key.add(line.get(index - 1));
		}
		return key;
	}
}
