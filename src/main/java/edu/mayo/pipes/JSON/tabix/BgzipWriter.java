package edu.mayo.pipes.JSON.tabix;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import net.sf.samtools.util.BlockCompressedOutputStream;

import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.history.History;

public class BgzipWriter extends AbstractPipe<History, History> {
	
	private String mBgzipOutFilePath;
	private BlockCompressedOutputStream mBgzipOutStream;
	private boolean mIsCreateTabix = true;
	
	/**
	 * Write all lines to a bgzip file
	 * @param bgzipOutFilePath Path to bgzip file
	 * @param isCreateTabix Create a tabix file from the bgzip file when done
	 */
	public BgzipWriter(String bgzipOutFilePath, boolean isCreateTabix) {
		mBgzipOutFilePath = bgzipOutFilePath; 
		mBgzipOutStream = new BlockCompressedOutputStream(bgzipOutFilePath, 9);
		mIsCreateTabix = isCreateTabix;
	}
	
	@Override
	protected History processNextStart() throws NoSuchElementException {
		try {
			// If no more input, then close the bgzip stream and throw exception
			if(! this.hasNext() ) {
				mBgzipOutStream.close();
				if(mIsCreateTabix)
					createTabixFile();
				throw new NoSuchElementException("No more data");
			}
			
			// Get next data line and write it out to the bgzip file, then return it
			History nextLine = this.next();
			mBgzipOutStream.write(merge(nextLine).getBytes());
			return nextLine;
		}catch(NoSuchElementException noSuchExc) {
			throw new NoSuchElementException("No more data");
		}catch(IOException e) {
			e.printStackTrace();
			throw new NoSuchElementException("Error writing to bgzip file");
		}catch(Exception e) {
			e.printStackTrace();
			throw new NoSuchElementException("Error creating tabix file");
		}
	}
	
	private void createTabixFile() throws Exception {
		File bgzipOutFile = new File(mBgzipOutFilePath);
		TabixWriter tabixWriter = new TabixWriter(bgzipOutFile, TabixWriter.VCF_CONF);
		tabixWriter.createIndex(bgzipOutFile);
	}

	private String merge(History history) {
		StringBuilder strBuilder = new StringBuilder();
		for(String col : history) {
			strBuilder.append(col + "\t");
		}
		String str = strBuilder.toString();
		if(str.endsWith("\t"))
			str = str.substring(0,str.length()-1);
		return str;
	}
}
