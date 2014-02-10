package edu.mayo.pipes.string;

import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.history.History;

/**
 * Useful utility for trim spaces around tabbed columns.
 * For example (where -> is tab, and _ is space):
 *   A->__B_->C->____D->E__
 * Becomes
 *   A->B->C->D->E
 * @author Michael Meiners (m054457)
 * Date created: Jan 13, 2014
 * @param <S>
 */
public class TrimSpacesPipe<S>  extends AbstractPipe<S, S>{

	/** Given a line with tab-delimited columns, remove all spaces before and after each column
	 * For example (where -> is tab, and _ is space):
	 *   A->__B_->C->____D->E__
	 * Becomes
	 *   A->B->C->D->E
	 * @see com.tinkerpop.pipes.AbstractPipe#processNextStart()
	 */
	@Override
	protected S processNextStart() throws NoSuchElementException {
		S line = this.starts.next();
		if( line instanceof String ) {
			return (S)(trimSpaces((String)line));
		} else if( line instanceof History ) {
			return (S)(trimSpaces((History)line));
		} else {
			return line;
		}
	}
	
	protected String trimSpaces(String line) {
		// If the line starts with "##", then just return it - don't modify metadata lines
		// (since they could possibly contain tabs within descriptions)
		// The header lines starting with "#", however, SHOULD be modified
		if( line.startsWith("##") )
			return line;
		
		String[] cols = line.split("\t");
		StringBuilder str = new StringBuilder();
		for(String s : cols) {
			str.append(s.trim()).append("\t");
		}
		// Remove last tab
		if( str.length() > 0  &&  str.charAt(str.length()-1) == '\t')
			str.deleteCharAt(str.length()-1);
		return str.toString();
	}

	protected History trimSpaces(History line) {
		// If the line starts with "##", then just return it - don't modify metadata lines
		// (since they could possibly contain tabs within descriptions)
		// The header lines starting with "#", however, SHOULD be modified
		if( line.size() > 0 && line.get(0).startsWith("##") )
			return line;
		
		for(int i=0; i < line.size(); i++) {
			line.set(i, line.get(i).trim());
		}
		return line;
	}

}
