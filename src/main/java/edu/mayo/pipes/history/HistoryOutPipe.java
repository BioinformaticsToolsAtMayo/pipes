package edu.mayo.pipes.history;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.bioinformatics.vocab.Undefined;

/**
 * Serializes a History object into a header that appears before the data.
 * 
 * @author duffp
 *
 */
public class HistoryOutPipe extends AbstractPipe<History, String>{

	private static final String FIELD_DELIMITER = "\t";
	
	// FLAG that determines whether the queue has the prepend lines or not
	private boolean mHasPrepended = false;
	
	// QUEUE used to get the right order of Strings coming out of this pipe
	// the order should be:
	// 1. header lines
	// 2. data lines
	private List<String> mQueue = new ArrayList<String>();
	
	@Override
	protected String processNextStart() throws NoSuchElementException {
		
		// prepend to the queue of rows the header and 1st data row
		if (!mHasPrepended) {
			
			// it's necessary to pull the 1st row to get things started
			History history = this.starts.next();

            List<String> originalHeader = History.getMetaData().getOriginalHeader();
            int p = 1; //by default add all header lines except for the LAST line which is the old column header line
            if(originalHeader.size() > 0){
                if(originalHeader.get(originalHeader.size()-1).startsWith("##")){ // the last header row is not a column header but a ## metadata line, don't delete
                    p = 0;
                }
            }
            // add the header lines to the queue first so they appear in the
			// output first
			final int origHeaderSize = History.getMetaData().getOriginalHeader().size();
			// add all header lines except for LAST line, which is the column header row
			for (int i=0; i < (origHeaderSize - p); i++) {
				String headerLine = History.getMetaData().getOriginalHeader().get(i);
				mQueue.add(headerLine);					
			}
            List<ColumnMetaData> headerCols = History.getMetaData().getColumns();
            while(history.size() > headerCols.size()){
                ColumnMetaData cmd = new ColumnMetaData("#" + Undefined.UNKNOWN + "_" + (new Integer(headerCols.size()+1)).toString());
                headerCols.add(cmd);
            }
			// add a new generated column header row, even if the original header was blank
			mQueue.add(History.getMetaData().getColumnHeaderRow(FIELD_DELIMITER));

			// need to also queue up the 1st data row
			String firstDataRow = history.getMergedData(FIELD_DELIMITER);
			mQueue.add(firstDataRow);			
			
			mHasPrepended = true;
		}
		
		if (mQueue.size() > 0) {
						
			// pop off first item from queue
			return mQueue.remove(0);
			
		} else {
			
			// QUEUE has been exhausted, now just append the data rows 			
			History history = this.starts.next();
			String dataRow = history.getMergedData(FIELD_DELIMITER);; 
			return dataRow;
			
		}
	}

	@Override
	/**
	 * Need to override this method so that it looks at the QUEUE
	 * until it is exhausted.
	 */
	public boolean hasNext() {
		if (mQueue.size() > 0) {
			return true;
		} else {
			return super.hasNext();
		}
	}

}
