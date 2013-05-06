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
	
	/**
	 * Constructor
	 *
	 * @param fieldSpec
	 * 		{@link FieldSpecification} that specifies which fields will be compressed. <p/>
	 * @param delimiter
	 * 		Delimiter used to concat multiple row values for 1 column into a single cel value.
	 */
	public CompressPipe(FieldSpecification fieldSpec, String delimiter)
	{
		mFieldSpec = fieldSpec;
		mDelimiter = delimiter;
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
		
		// 1ST line
		List<String> firstLine = lines.remove(0);
		
		// list of StringBuilders, 1 per column for final compressed line
		List<StringBuilder> builders = new ArrayList<StringBuilder>();
		for (int i=0; i < firstLine.size(); i++)
		{
			// initialize StringBuilders to the 1ST line values
			builders.add(new StringBuilder(firstLine.get(i)));
		}

		// for-each subsequent line
		for (List<String> line: lines)
		{
			// for-each column
			for (int col=0; col < line.size(); col++)
			{
				int field = col + 1;
				if (mCompressFields.contains(field))
				{
					String colValue = line.get(col);
					builders.get(col).append(mDelimiter);					
					builders.get(col).append(colValue);
				}
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
