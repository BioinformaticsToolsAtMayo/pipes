package edu.mayo.pipes.history;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

/**
 * Processes incoming String data and deserializes it into a History object.
 * 
 * @author duffp
 *
 */
public class HistoryInPipe extends AbstractPipe<String, History> {

	private final String COL_DELIMITER = "\t";
	
	@Override
	protected History processNextStart() throws NoSuchElementException {

		String line = this.starts.next();

		History history = new History();

		if (history.isMetaDataInitialized() == false) {
			// fast forward and capture header rows until we hit the first data
			// row
			List<String> headerRows = new ArrayList<String>();
			while (line.startsWith("#")) {
				headerRows.add(line);

				line = this.starts.next();
			}
			HistoryMetaData hMeta = new HistoryMetaData(headerRows);

			// process column header if present
			if (headerRows.size() > 0) {
				String colHeaderLine = headerRows.get(headerRows.size() - 1);

				// trim off leading #
				colHeaderLine = colHeaderLine.substring(1);

				for (String colName : colHeaderLine.split(COL_DELIMITER)) {
					ColumnMetaData cmd = new ColumnMetaData(colName);
					hMeta.getColumns().add(cmd);
				}
			} else {
				int numCols = line.split(COL_DELIMITER).length;
				// if there is no column header, just mark each column as
				// UNKNOWN
				for (int i = 1; i <= numCols; i++) {
					ColumnMetaData cmd = new ColumnMetaData("UNKNOWN_" + i);
					hMeta.getColumns().add(cmd);
				}
			}

			history.setMetaData(hMeta);
		}

		// split data row, add to history
		String[] colDataArr = line.split(COL_DELIMITER);
		for (String colData : colDataArr) {
			history.add(colData);
		}

		return history;
	}
}
