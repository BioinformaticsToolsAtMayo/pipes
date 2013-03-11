/**
 * bior_pipes
 *
 * <p>@author Gregory Dougherty</p>
 * Copyright Mayo Clinic, 2011
 *
 */
package edu.mayo.pipes.iterators;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

/**
 * <p>@author Gregory Dougherty</p>
 *
 */
public class AnyFileLineIterator extends FileLineIterator
{
	
	/**
	 * @param filename
	 * @throws IOException
	 */
	public AnyFileLineIterator (String filename) throws IOException
	{
		super (filename);
	}
	
	
	/* (non-Javadoc)
	 * @see edu.mayo.pipes.iterators.FileLineIterator#open(java.lang.String)
	 */
	@Override
	public BufferedReader open (String fileName) throws IOException
	{
		File		inFile = new File (fileName);
		Compressor	comp = new Compressor (inFile, null);
		return br = comp.getReader ();
	}
	
}
