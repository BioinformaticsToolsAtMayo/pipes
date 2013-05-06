package edu.mayo.pipes.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Selection of fields done via a UNIX cut style field specification.
 * 
 * NOTE: fields are 1-based.
 * 
 * @author duffp
 *
 */
public class FieldSpecification
{
	public enum FieldType
	{
		/**
		 * Fields that match the specification.
		 */
		MATCH,
		
		/**
		 * Fields that do not match the specification (complement of MATCH).
		 */
		NON_MATCH
	}
	
	/**
	 * The direction for the Field numbering.
	 */
	public enum FieldDirection
	{
		/**
		 * Fields are ordered 1, 2, 3, 4, 5, 6, ... where Field 1 is the first item.
		 */
		LEFT_TO_RIGHT,

		/**
		 * Fields are ordered ..., 6, 5, 4, 3, 2, 1 where Field 1 is the last item.
		 */		
		RIGHT_TO_LEFT
	}
	
	enum Type
	{
		NTH_ONLY,
		NTH_TO_END,
		NTH_TO_MTH,
		FIRST_TO_MTH
	}

	class FieldRange
	{
		public Type type;
		public Integer nthField;
		public Integer mthField;
	}	
	
	private List<FieldRange> mRanges = new ArrayList<FieldRange>();
	
	private FieldDirection mDirection;

	/**
	 * Constructor
	 * <p/>
	 * NOTE: Fields are numbered left to right (e.g. 1, 2, 3, 4, 5, etc...).
	 *
	 * @param spec
	 * 		The specification.  A specification is made up of one range, or many
	 * 		ranges separated by commas.  Each range is one of: <p/>
	 * 
	 *<table border=1>
	 *<tr><td> N   </td><td> Nth field, counted from 1 </td></tr>
	 *<tr><td> N-  </td><td> from Nth field, to end of line </td></tr>
	 *<tr><td> N-M </td><td> from Nth to Mth (included) field </td></tr>
	 *<tr><td> -M  </td><td> from first to Mth (included) field </td></tr>
	 *</table>
	 *<p/>
	 *
	 */
	public FieldSpecification(String spec)
	{
		this(spec, FieldDirection.LEFT_TO_RIGHT);
	}
		
	/**
	 * Constructor
	 *
	 * @param spec
	 * 		The specification.  A specification is made up of one range, or many
	 * 		ranges separated by commas.  Each range is one of: <p/>
	 * 
	 *<table border=1>
	 *<tr><td> N   </td><td> Nth field, counted from 1 </td></tr>
	 *<tr><td> N-  </td><td> from Nth field, to end of line </td></tr>
	 *<tr><td> N-M </td><td> from Nth to Mth (included) field </td></tr>
	 *<tr><td> -M  </td><td> from first to Mth (included) field </td></tr>
	 *</table>
	 *<p/>
	 *
	 * @param direction
	 *		The direction for the Field numbering specified using a {@link FieldDirection}.
	 */
	public FieldSpecification(String spec, FieldDirection direction)
	{
		mDirection = direction;
		
		for (String rangeStr: spec.split(","))
		{
			FieldRange range;
			if (rangeStr.contains("-"))
			{
				if (rangeStr.charAt(0) == '-')
				{
					range = new FieldRange();
					range.type = Type.FIRST_TO_MTH;
					range.mthField = new Integer(rangeStr.substring(1));
				}
				else if (rangeStr.charAt(rangeStr.length() - 1) == '-')
				{
					range = new FieldRange();
					range.type = Type.NTH_TO_END;
					range.nthField = new Integer(rangeStr.substring(0, rangeStr.length() - 1));					
				}
				else
				{
					range = new FieldRange();
					range.type = Type.NTH_TO_MTH;
					String[] arr = rangeStr.split("-");
					range.nthField = new Integer(arr[0]);					
					range.mthField = new Integer(arr[1]);					
				}				
			}
			else
			{
				range = new FieldRange();
				range.type = Type.NTH_ONLY;
				range.nthField = new Integer(rangeStr);
			}
			mRanges.add(range);
		}
	}

	/**
	 * Gets a list of fields based on this {@link FieldSpecification}.
	 * 
	 * @param numFields
	 * 		total number of fields
	 * @return
	 * 		A {@link java.util.Map} where the keys are defined by the enum 
	 * 		{@link FieldType} and the values are a list of integers presenting
	 * 		the fields.
	 */
	public Map<FieldType, List<Integer>> getFields(int numFields)
	{
		Map<FieldType, List<Integer>> m = new HashMap<FieldType, List<Integer>>();
		
		// determine matches
		List<Integer> matches = new ArrayList<Integer>();
		for (FieldRange range: mRanges)
		{
			// assume NTH and MTH fields have direction LEFT_TO_RIGHT
			Integer nthField = range.nthField;
			Integer mthField = range.mthField;
			
			// orient NTH and MTH fields to be LEFT_TO_RIGHT if needed
			if (mDirection.equals(FieldDirection.RIGHT_TO_LEFT))
			{
				if (range.nthField != null)
					nthField = numFields - range.nthField + 1;
				if (range.mthField != null)
					mthField = numFields - range.mthField + 1;
			}
			
			switch (range.type)
			{
				case NTH_ONLY:
					matches.add(nthField);
					break;
				case FIRST_TO_MTH:
					for (int i=1; i <= mthField; i++)
					{
						matches.add(i);						
					}
					break;
				case NTH_TO_END:
					for (int i=nthField; i <= numFields; i++)
					{
						matches.add(i);						
					}
					break;
				case NTH_TO_MTH:
					int start = nthField;
					int end = mthField;
					if (mDirection.equals(FieldDirection.RIGHT_TO_LEFT))
					{
						// reverse
						start = mthField;
						end = nthField;						
					}

					for (int i=start; i <= end; i++)
					{
						matches.add(i);						
					}
					break;
			}
		}
		Collections.sort(matches);
		m.put(FieldType.MATCH, matches);
		
		// complement are non-matches
		List<Integer> nonMatches = new ArrayList<Integer>();
		for (int i=1; i <= numFields; i++)
		{
			if (!matches.contains(i))
				nonMatches.add(i);
		}		
		Collections.sort(nonMatches);
		m.put(FieldType.NON_MATCH, nonMatches);
		
		return m;
	}	
}