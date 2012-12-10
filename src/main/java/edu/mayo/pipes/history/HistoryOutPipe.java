package edu.mayo.pipes.history;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

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

			// add the header lines to the queue first so they appear in the
			// output first
			for (String headerLine: History.getMetaData().getOriginalHeader()) {
				mQueue.add(headerLine);
			}

			// need to also queue up the 1st data row
			String firstDataRow = history.getMergedData(FIELD_DELIMITER);
			mQueue.add(firstDataRow);			
			
			mHasPrepended = true;
		}
		
		if (mQueue.size() > 0) {
			
			// handle column header row
			// NOTE: 2nd to last item in QUEUE is the column header
			if (mQueue.size() == 2) {

				// throw away original column header row
				mQueue.remove(0);
				
				//  reconstruct column header row dynamically based on meta data
				StringBuilder sb = new StringBuilder();
				sb.append("#");
				final int numCols = History.getMetaData().getColumns().size();
				for (int i=0; i < numCols; i++) {
					ColumnMetaData cmd = History.getMetaData().getColumns().get(i);
					sb.append(cmd.getColumnName());
					
					if (i < (numCols - 1)) {
						sb.append(FIELD_DELIMITER);
					}					
				}
				String colHeaderRow = sb.toString();				
				return colHeaderRow;
			}
			
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
