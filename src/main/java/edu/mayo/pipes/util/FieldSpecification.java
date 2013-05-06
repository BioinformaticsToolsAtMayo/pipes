package edu.mayo.pipes.util;

import java.util.ArrayList;
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
	
	/**
	 * Constructor
	 *
	 * @param spec
	 * 		The specification.  A specification is made up of one range, or many
	 * 		ranges separated by commas.  Each range is one of: <p/>
	 * 
	 *<table>
	 *<tr><td> N   </td><td> Nth field, counted from 1 </td></tr>
	 *<tr><td> N-  </td><td> from Nth field, to end of line </td></tr>
	 *<tr><td> N-M </td><td> from Nth to Mth (included) field </td></tr>
	 *<tr><td> -M  </td><td> from first to Mth (included) field </td></tr>
	 *</table>
	 */
	public FieldSpecification(String spec)
	{
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
			switch (range.type)
			{
				case NTH_ONLY:
					matches.add(range.nthField);
					break;
				case FIRST_TO_MTH:
					for (int i=1; i <= range.mthField; i++)
					{
						matches.add(i);						
					}
					break;
				case NTH_TO_END:
					for (int i=range.nthField; i <= numFields; i++)
					{
						matches.add(i);						
					}
					break;
				case NTH_TO_MTH:
					for (int i=range.nthField; i <= range.mthField; i++)
					{
						matches.add(i);						
					}
					break;
			}
		}		
		m.put(FieldType.MATCH, matches);
		
		// complement are non-matches
		List<Integer> nonMatches = new ArrayList<Integer>();
		for (int i=1; i <= numFields; i++)
		{
			if (!matches.contains(i))
				nonMatches.add(i);
		}		
		m.put(FieldType.NON_MATCH, nonMatches);
		
		return m;
	}	
}