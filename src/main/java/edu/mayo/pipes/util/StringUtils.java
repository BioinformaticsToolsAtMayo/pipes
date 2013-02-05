package edu.mayo.pipes.util;

import java.util.ArrayList;
import java.util.List;

public class StringUtils {

	/**
	 * Alternative to the java.lang.String.split() method.  This implementation
	 * will account for missing data between delimiters, which are empty Strings.
	 * 
	 * @param s         The string to be split.
	 * @param delimiter The delimiter to be used for splitting the given string.
	 * @return A string array containing the fields from the original string.
	 */
	public static String[] safeSplit(String s, String delimiter) {
		List<String> list = new ArrayList<String>();

		int fromIdx = 0;
		int delimiterIdx = 0;
		
		do {
			// get index of next delimiter
			delimiterIdx = s.indexOf(delimiter, fromIdx);
			
			// add the next token
			if (delimiterIdx != -1) {
				list.add(s.substring(fromIdx, delimiterIdx));
			} else {
				// last token
				list.add(s.substring(fromIdx));				
			}
			
			// update the from index so that the String.indexOf search
			// ignores the token that was just added
			fromIdx = delimiterIdx + 1;
			
		} while (delimiterIdx != -1);
		
		return list.toArray(new String[0]);
	}
}