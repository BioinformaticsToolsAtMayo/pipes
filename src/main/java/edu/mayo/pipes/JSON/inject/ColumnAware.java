package edu.mayo.pipes.JSON.inject;

/**
 * Interface to objects that work with a column.
 */
public interface ColumnAware {

	/**
	 * Gets the column 1-based number (e.g. the first column is 1, second column is 2, etc...)
	 * @return
	 */
	public int getColumn();
	
}
