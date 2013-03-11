/**
 * ExomeTableBrowser
 *
 * Copyright 2007 by Greg Dougherty
 * License to be determined.
 */

package edu.mayo.pipes.iterators;

import java.io.*;
import java.util.zip.*;
import org.apache.commons.compress.compressors.gzip.*;
import org.apache.commons.compress.compressors.bzip2.*;

/**
 * Class that seamlessly handles reading and writing Zipped, Gzipped, and Bzipped files, using Java's 
 * built in code for dealing with Zipped files, and Apache's code for dealing with BZipped and Gzipped 
 * files.  It does this by looking at the file suffix, returning a plain Buffered Reader / Writer if 
 * the file has a suffix that is not recognized as a compressed file.
 * 
 * @author Greg Dougherty
 */

public class Compressor
{
	private File			inFile, outFile;
	private BufferedReader	reader;
	private BufferedWriter	writer;
	private int				comp;

	private static final int	kNotAssigned = 0;
	/**
	 * Signifies an uncompressed file, or at least one whose compression we don't understand.
	 */
	public static final int		kNoCompression = 1;
	/**
	 * Signifies a file using Zip compression.
	 */
	public static final int		kZipCompression = 2;
	/**
	 * Signifies a file using GZip compression.
	 */
	public static final int		kGZipCompression = 3;
	/**
	 * Signifies a file using BZip compression.
	 */
	public static final int		kBZipCompression = 4;

	private static final int	kBZipMagicBytes = 2;
	private static final byte[]	kBZipHeaderMagic = {'B', 'Z'};

	private static final String		gZipSuffix = ".zip";
	private static final String[]	gGZipSuffixes = {".gz", ".gzip"};
	private static final String[]	gBZipSuffixes = {".bz", ".bz2", ".tbz", ".tbz2"};


	/**
	 * @param inFile	File to read from, that might be zip, bzip or gzip compressed, or null
	 * @param outFile	File to write to, that might be zip, bzip or gzip compressed, or null
	 * @throws IOException if there's a problem building the file accessors
	 */
	public Compressor (File inFile, File outFile) throws IOException
	{
		super ();
		this.inFile = inFile;
		this.outFile = outFile;
		reader = null;
		writer = null;
		comp = kNotAssigned;

		buildAccessors (false);
	}


	/**
	 * @param inFile	File to read from, that might be zip, bzip or gzip compressed, or null
	 * @param outFile	File to write to, that might be zip, bzip or gzip compressed, or null
	 * @param append	If true, then if writing to a non-compressed file will append data
	 * to the file rather than overwriting it.
	 * @throws IOException if there's a problem building the file accessors
	 */
	public Compressor (File inFile, File outFile, boolean append) throws IOException
	{
		super ();
		this.inFile = inFile;
		this.outFile = outFile;
		reader = null;
		writer = null;
		comp = kNotAssigned;

		buildAccessors (append);
	}


	/**
	 * @param inStream	Stream to read from, that might be zip, gzip or bzip compressed
	 * @param name		Name of the item behind the stream, so we can figure out the compression
	 */
	public void setInputStream (InputStream inStream, String name)
	{
		try
		{
			String	suffix = getSuffix (name);
			if ((suffix != null) && (suffix.length () > 0))
			{
				if (isZipSuffix (suffix))
					makeZipReader (inStream);
				else if (isGZipSuffix (suffix))
					makeGZipReader (inStream);
				else if (isBZipSuffix (suffix))
					makeBZipReader (inStream);
			}
		}
		catch (IOException oops)
		{
			// Ignore exceptions, just make the normal reader
		}

		if (reader == null)
			reader = new BufferedReader (new InputStreamReader (inStream));
	}


	/**
	 * @return the reader created for the input file
	 */
	public final BufferedReader getReader ()
	{
		return reader;
	}


	/**
	 * @return the writer created for the output file
	 */
	public final BufferedWriter getWriter ()
	{
		return writer;
	}


	/**
	 * Accessor function to return the type of compression this file is using
	 *
	 * @return	an int describing the compression, if any
	 */
	public final int getCompression ()
	{
		return comp;
	}


	/**
	 * Static function to tell the caller if the file is / will be compressed
	 *
	 * @param testFile	File to test
	 * @return kZipCompression if a zip, kGZipCompression if gZipped, kBZipCompression if bZipped
	 * kBZipCompression if bZipped, else kNoCompression
	 */
	public static final int compressionUsed (final File testFile)
	{
		String	suffix = getSuffix (testFile);
		if ((suffix != null) && (suffix.length () > 0))
		{
			if (isZipSuffix (suffix))
				return kZipCompression;
			if (isGZipSuffix (suffix))
				return kGZipCompression;
			if (isBZipSuffix (suffix))
				return kBZipCompression;
		}

		return kNoCompression;
	}


	/**
	 * Routine to close the current reader, and get a new reader at the start of the file.
	 *
	 * @return	A new buffered reader aimed at the start of the file
	 * @throws IOException
	 */
	public final BufferedReader resetReader () throws IOException
	{
		File	outHold = outFile;
		outFile = null;
		reader.close ();
		reader = null;

		buildAccessors (false);

		outFile = outHold;
		return reader;
	}


	/**
	 * Return the suffix used by files with this type of compression, or an empty string if
	 * no compression.
	 *
	 * @param compression	type of compression
	 * @return	String containing the suffix.  Empty string if no compression
	 */
	public static final String getSuffix (int compression)
	{
		switch (compression)
		{
			case kZipCompression:
				return gZipSuffix;

			case kGZipCompression:
				return gGZipSuffixes[0];

			case kBZipCompression:
				return gBZipSuffixes[0];
		}

		return "";
	}


	/**
	 * Extract the suffix from the file name and return it
	 *
	 * @param file	File to process
	 * @return	The file's suffix, or an empty string if no suffix
	 */
	private static final String getSuffix (File file)
	{
		String	name = file.getName ();
		return getSuffix (name);
	}


	/**
	 * Extract the suffix from the name and return it
	 *
	 * @param name	Name to process
	 * @return	The name's suffix, or an empty string if no suffix
	 */
	private static final String getSuffix (String name)
	{
		int		index = name.lastIndexOf ('.');
		String	end;

		if (index >- 0)
			end = name.substring (index);
		else
			end = "";

		return end;
	}


	/**
	 * Figure out if this suffix can represent a zip file
	 *
	 * @param suffix	Suffix of the file we're testing
	 * @return	True if one of our allowed Zip suffixes, else false.  Currently, ".zip" is the
	 * only allowed suffix
	 */
	private static final boolean isZipSuffix (String suffix)
	{
		return (suffix.equals (gZipSuffix));
	}


	/**
	 * Figure out if this suffix can represent a gzip file
	 *
	 * @param suffix	Suffix of the file we're testing
	 * @return	True if one of our allowed GZip suffixes, else false.  Currently, ".gz" and
	 * ".gzip" are the only allowed suffixes
	 */
	private static final boolean isGZipSuffix (String suffix)
	{
		int	i, numSuffix = gGZipSuffixes.length;
		for (i = 0; i < numSuffix; ++i)
		{
			if (suffix.equals (gGZipSuffixes[i]))
				return true;
		}

		return false;
	}


	/**
	 * Figure out if this suffix can represent a bzip file
	 *
	 * @param suffix	Suffix of the file we're testing
	 * @return	True if one of our allowed BZip suffixes, else false.  Currently, ".bz", ".bz2",
	 * ".tbz" and ".tbz2" are the only allowed suffixes
	 */
	private static final boolean isBZipSuffix (String suffix)
	{
		int	i, numSuffix = gBZipSuffixes.length;
		for (i = 0; i < numSuffix; ++i)
		{
			if (suffix.equals (gBZipSuffixes[i]))
				return true;
		}

		return false;
	}


	/**
	 * Create the accessors for the (up to) two files passed in
	 *
	 * @param append	If true, append to the end of the write file instead of re-writing it
	 * @throws IOException
	 */
	private void buildAccessors (boolean append) throws IOException
	{
		String	suffix;

		if (inFile != null)
		{
			suffix = getSuffix (inFile);
			try
			{
				if ((suffix != null) && (suffix.length () > 0))
				{
					if (isZipSuffix (suffix))
						makeZipReader ();
					else if (isGZipSuffix (suffix))
						makeGZipReader ();
					else if (isBZipSuffix (suffix))
						makeBZipReader ();
				}
			}
			catch (IOException e)
			{
				// Ignore exceptions, just make the normal reader
			}

			if (reader == null)
				makeNormalReader ();
		}

		if (outFile != null)
		{
			suffix = getSuffix (outFile);
			try
			{
				if ((suffix != null) && (suffix.length () > 0))
				{
					if (isZipSuffix (suffix))
						makeZipWriter ();
					else if (isGZipSuffix (suffix))
						makeGZipWriter ();
					else if (isBZipSuffix (suffix))
						makeBZipWriter ();
				}
			}
			catch (IOException e)
			{
				// Ignore exceptions, just make the normal reader
			}

			if (writer == null)
				makeNormalWriter (append);
		}
	}


	/**
	 * Called for reading files that aren't compressed, or whose compression we don't understand.
	 *
	 * @throws IOException
	 */
	private void makeNormalReader () throws IOException
	{
		FileReader rFile = new FileReader (inFile);
		reader = new BufferedReader (rFile);
	}


	/**
	 * Called for writing files that aren't compressed, or whose compression we don't understand.
	 *
	 * @param append	If true, append to the end of the write file instead of re-writing it
	 * @throws IOException
	 */
	private void makeNormalWriter (boolean append) throws IOException
	{
		FileWriter wFile = new FileWriter (outFile, append);
		writer = new BufferedWriter (wFile);
	}


	/**
	 * Make a Zip file reader, and set it to read the first entry in the zip file.<br/>
	 * All other entries in the zip file are ignored
	 *
	 * @throws IOException
	 */
	private void makeZipReader () throws IOException
	{
		if (inFile == null)
			return;

		// Get the Zip file ready to read its one and only entry
		makeZipReader (new FileInputStream (inFile));
	}


	/**
	 * Make a Zip file reader, and set it to read the first entry in the zip file.<br/>
	 * All other entries in the zip file are ignored
	 *
	 * @param inStream		Stream to read from
	 * @throws IOException
	 */
	private void makeZipReader (InputStream inStream) throws IOException
	{
		ZipInputStream	zipRead = new ZipInputStream (inStream);
//		ZipEntry		zE = zipRead.getNextEntry ();
		zipRead.getNextEntry ();	// Ignore result, we don't use

		// Now ready to read file, so create readers to do that
		InputStreamReader rStream = new InputStreamReader (zipRead);
		reader = new BufferedReader (rStream);
		comp = kZipCompression;
	}


	/**
	 * Create a single entry Zip archive, and prepare it for writing
	 *
	 * @throws IOException
	 */
	private void makeZipWriter () throws IOException
	{
		if (outFile == null)
			return;

		FileOutputStream	outFileStream = new FileOutputStream (outFile);
		ZipOutputStream		zipWrite = new ZipOutputStream (outFileStream);
		ZipEntry			zE;

		// Setup the zip writing things
		zipWrite.setMethod (ZipOutputStream.DEFLATED);
		zipWrite.setLevel (9); // Max compression
		zE = new ZipEntry ("Default");
		zipWrite.putNextEntry (zE);
		// Now can attach the writer to write to this zip entry
		OutputStreamWriter wStream = new OutputStreamWriter (zipWrite);
		writer = new BufferedWriter (wStream);
		comp = kZipCompression;
	}


	/**
	 * Create a GZip file reader, open the file, and prepare it for reading
	 *
	 * @throws IOException
	 */
	protected void makeGZipReader () throws IOException
	{
		if (inFile == null)
			return;

		makeGZipReader (new FileInputStream (inFile));
	}


	/**
	 * Create a GZip file reader, open the file, and prepare it for reading
	 *
	 * @param inStream		Stream to read from
	 * @throws IOException
	 */
	protected void makeGZipReader (InputStream inStream) throws IOException
	{
		GzipCompressorInputStream zipRead = new GzipCompressorInputStream (inStream, true);
		// Now ready to read file, so create readers to do that
		InputStreamReader rStream = new InputStreamReader (zipRead);
		reader = new BufferedReader (rStream);
		comp = kGZipCompression;
	}


	/**
	 * Create a GZip file archive and prepare it for writing
	 *
	 * @throws IOException
	 */
	protected void makeGZipWriter () throws IOException
	{
		if (outFile == null)
			return;

		FileOutputStream			outFileStream = new FileOutputStream (outFile);
		GzipCompressorOutputStream	zipWrite = new GzipCompressorOutputStream (outFileStream);
		// Now can attach the writer to write to this zip entry
		OutputStreamWriter wStream = new OutputStreamWriter (zipWrite);
		writer = new BufferedWriter (wStream);
		comp = kGZipCompression;
	}


	/**
	 * Create a BZip file reader, open the file, and prepare it for reading
	 *
	 * @throws IOException
	 */
	protected void makeBZipReader () throws IOException
	{
		if (inFile == null)
			return;

		makeBZipReader (new FileInputStream (inFile));
	}


	/**
	 * Create a BZip file reader, open the file, and prepare it for reading
	 *
	 * @param inStream		Stream to read from
	 * @throws IOException
	 */
	protected void makeBZipReader (InputStream inStream) throws IOException
	{
		BufferedInputStream	inBuffStream = new BufferedInputStream (inStream);
		inBuffStream.skip (kBZipMagicBytes);	// Have to skip first two bytes to let the Ant code work
		BZip2CompressorInputStream	zipRead = new BZip2CompressorInputStream (inBuffStream);
		// Now ready to read file, so create readers to do that
		InputStreamReader	rStream = new InputStreamReader (zipRead);
		reader = new BufferedReader (rStream);
		comp = kBZipCompression;
	}


	/**
	 * Create a BZip file archive and prepare it for writing
	 *
	 * @throws IOException
	 */
	protected void makeBZipWriter () throws IOException
	{
		if (outFile == null)
			return;

		FileOutputStream		outFileStream = new FileOutputStream (outFile);
		BufferedOutputStream	outBuffStream = new BufferedOutputStream (outFileStream);
		outBuffStream.write (kBZipHeaderMagic);
		BZip2CompressorOutputStream	zipWrite = new BZip2CompressorOutputStream (outBuffStream);
		// Now can attach the writer to write to this zip entry
		OutputStreamWriter wStream = new OutputStreamWriter (zipWrite);
		writer = new BufferedWriter (wStream);
		comp = kBZipCompression;
	}

}
