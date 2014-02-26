package edu.mayo.pipes.history;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.history.ColumnMetaData.Type;
import edu.mayo.pipes.util.FieldSpecification.FieldType;
import edu.mayo.pipes.util.metadata.AddMetadataLines;
import edu.mayo.pipes.util.metadata.AddMetadataLines.BiorMetaControlledVocabulary;
import edu.mayo.pipes.util.metadata.Metadata;

/**
 * Processes incoming String data and deserializes it into a History object.
 * 
 * @author duffp
 * dquest
 *
 */
public class HistoryInPipe extends AbstractPipe<String, History> {

    List<Metadata> metadata = new ArrayList<Metadata>();
    private int mNumColumns = 0;

    /**
     * Make a new HistoryInPipe with metadata operations pending
     * metadata operations allow the ## lines to be modified
     * on the first pipe (HistoryInPipe) in the pipeline
     * @param md
     */
    public HistoryInPipe(Metadata md){
         metadata.add(md);
    }

    public HistoryInPipe(List<Metadata> md){
        metadata = md;
    }

	private int expand2NumCols = -1;
    public HistoryInPipe(){
    	History.clearMetaData();
        expand2NumCols = -1;
    }
    
    public HistoryInPipe(int expand2NumCols){
    	History.clearMetaData();
        this.expand2NumCols = expand2NumCols;
    }
    
    /** Allows pipeline to be reused - just call this between different pipe runs */
    @Override
	public void reset() {
		super.reset();
		History.clearMetaData();
	}

	private final String COL_DELIMITER = "\t";
	
	@Override
	protected History processNextStart() throws NoSuchElementException
	{
		String line = this.starts.next();

		History history = new History();

		if (history.isMetaDataInitialized() == false)
		{


			// fast forward and capture header rows until we hit the first data row
			List<String> headerRows = new ArrayList<String>();
			while (line.startsWith("#"))
			{
				headerRows.add(line);

				try
				{
					line = this.starts.next();
				}
				catch (NoSuchElementException e)
				{
					// attempted to grab next line, but there are no more
					// means there are no data lines after header lines

					// # of columns based on last header row
					int numCols = line.split(COL_DELIMITER).length;
					
					initializeMetaData(history, headerRows, numCols);
					
					// re-throw to tell next pipe there are no more History objects
					throw e;
				}
			}
			
			// # of columns based on 1st data line
			mNumColumns = line.split(COL_DELIMITER).length;
			
			initializeMetaData(history, headerRows, mNumColumns);
            insertBIORLines(history);
		}

		return new History(line);
	}
	
	/**
	 * Initializes the History metadata.
	 * 
	 * @param history
	 * @param headerRows
	 * @param numCols
	 */
	private void initializeMetaData(History history, List<String> headerRows, int numCols)
	{
		HistoryMetaData hMeta = new HistoryMetaData(headerRows);

		// process column header if present
		if (headerRows.size() > 0)
		{
			String colHeaderLine = headerRows.get(headerRows.size() - 1);

			// trim off leading #
			colHeaderLine = colHeaderLine.substring(1);

			for (String colName : colHeaderLine.split(COL_DELIMITER))
			{
				ColumnMetaData cmd = new ColumnMetaData(colName);
				hMeta.getColumns().add(cmd);
			}
		} 
		else 
		{
			if(numCols != this.expand2NumCols && this.expand2NumCols != -1)
			{
				while(history.size() < expand2NumCols)
				{
					history.add("");
				}
				numCols= this.expand2NumCols;
			}
			
			// if there is no column header, just mark each column as UNKNOWN
			for (int i = 1; i <= numCols; i++)
			{
				ColumnMetaData cmd = new ColumnMetaData("#UNKNOWN_" + i);
				hMeta.getColumns().add(cmd);
			}
		}

		history.setMetaData(hMeta);		
	}

    private AddMetadataLines amdl = new AddMetadataLines();
    public void insertBIORLines(History h)  {
        if(this.metadata.size() < 1){
        	return;  //do nothing we don't have to add header lines
        }
        
        HistoryMetaData hMeta = History.getMetaData();
        for(int i=0; i< this.metadata.size(); i++) {
            Metadata meta = this.metadata.get(i);
            //type = ToTJson
            if(meta.getCmdType().equals(Metadata.CmdType.ToTJson)){
                amdl.constructToTJsonLine(h, meta.getOperator(), meta.getCmdType().toString());
                ColumnMetaData cmd = new ColumnMetaData(AddMetadataLines.BiorMetaControlledVocabulary.BIOR + meta.getCmdType().toString());
                hMeta.getColumns().add(cmd);
            }
            //type = Drill
            else if(meta.getCmdType().equals(Metadata.CmdType.Drill)){
                String col = amdl.constructDrillLines(h, meta.getOperator(), meta.getColNum(), meta.getDrillPaths());

                List<ColumnMetaData> columnMeta = h.getMetaData().getColumns();
                int colToRemove = toZeroBasedCol(meta.getColNum(), columnMeta.size());
                
                //for each drill path, add the metadata
                for(String path : meta.getDrillPaths()){
                    ColumnMetaData cmd = new ColumnMetaData(AddMetadataLines.BiorMetaControlledVocabulary.BIOR + col + "." + path);
                    hMeta.getColumns().add(cmd);
                }

                //if we need to remove the metadata for the JSON column, do this
                if( ! meta.isKeepJSON()){
                    hMeta.getColumns().remove(colToRemove);
                }else{
                    //we have to remove the drill column and put it at the end
                    ColumnMetaData moveMe = hMeta.getColumns().get(colToRemove);
                    hMeta.getColumns().remove(colToRemove);
                    hMeta.getColumns().add(moveMe);
                }
            }
            //type = Query
            else if(meta.getCmdType().equals(Metadata.CmdType.Query)){
                try {
                    String col = amdl.constructQueryLine(h, meta.getFullCanonicalPath(), meta.getOperator());
                    ColumnMetaData cmd = new ColumnMetaData(AddMetadataLines.BiorMetaControlledVocabulary.BIOR + col);
                    hMeta.getColumns().add(cmd);
                } catch (Exception e) {
                    //if there does not exist a datasource.properties for a given catalog, then we need to modify
                    //the column but we can't add a header.  Note, this is not an error condition, but an expected
                    //code path that needs to be tested.
                    String col = amdl.constructQueryLineOnNoDatasourceProperties(h, meta.getFullCanonicalPath(), meta.getOperator());
                    ColumnMetaData cmd = new ColumnMetaData(AddMetadataLines.BiorMetaControlledVocabulary.BIOR + col);
                    hMeta.getColumns().add(cmd);
                }
            }
            //type = Tool
            else if(meta.getCmdType().equals(Metadata.CmdType.Tool)){
                try {
                    //if the file is not there, then we can't change the metadata to include the tool
                    String shortname = amdl.constructToolLine(h, meta.getDatasourcePath(), meta.getColumnsPath(), meta.getOperator());
                    ColumnMetaData cmd = new ColumnMetaData(AddMetadataLines.BiorMetaControlledVocabulary.BIOR+shortname);
                    hMeta.getColumns().add(cmd);
                } catch (IOException e) {
                   throw new RuntimeException("Stupid Developer! You need to put the path of the VEP/SNPEFF/Tool into your project and pass it to the metadata object\n  Look at HistoryInPipeTest for an example.\n CurrentPath: " + meta.getFullCanonicalPath());
                }
            }
            //type = Annotate
            else if( meta.getCmdType().equals(Metadata.CmdType.Annotate) ) {
            	try {
            		// If we do not have a catalog (for instance if it is tool like SnpEff or Vep), then use the tool's datasource and columns props files
            		if (meta.getDataSourceCanonicalPath()!= null)
            		    amdl.constructAnnotateLine(h, meta.getDataSourceCanonicalPath(), meta.getColumnCanonicalPath(),
            		    		meta.getDataSourceCanonicalPath(), meta.getOperator(), meta.getNewColNamesForDrillPaths(), meta.getDrillPaths());
            		else	
                        amdl.constructAnnotateLine(h, meta.getFullCanonicalPath(), meta.getOperator(), meta.getNewColNamesForDrillPaths(), meta.getDrillPaths());
                    //for each new column, add it to the column header row
                    for(String colName : meta.getNewColNamesForDrillPaths())
                        hMeta.getColumns().add(new ColumnMetaData(colName));
            	}catch(Exception e) {
            		throw new RuntimeException("Could not construct the metadata line for one of the columns.  " + e.getMessage());
            	}
            }
            // type = bior_compress
            else if( meta.getCmdType().equals(Metadata.CmdType.Compress) ) {
            	amdl.modifyCompressHeaders(meta, mNumColumns);
            }
        }
    }

    /** Take a 1-based column number from the user which may be either positive or negative (1...n, or -1...-n)
     *  and convert it to a positive 0-based column number (0...n-1) 
     *  For example, say we have 5 column headers and our column of interest is -2: this refers to column 3 (0-based)<br>
     *  <ul>
     *    <li>one-based-User-Column,  zero-based-col</li>
     *    <li>1                       0</li>
     *    <li>2                       1</li>
     *    <li>...</li>
     *    <li>5                       4</li>
     *    <li>-1                      4</li>
     *    <li>...</li>
     *    <li>-5                      0</li>
     *  </ul>
     * @param col
     * @return
     */
    private int toZeroBasedCol(int col, int numColHeaders) {
    	if( col == 0 )
            throw	new RuntimeException("You can't specify column number 0, use negative or positive numbers only!");
    	else if( col > 0 )
    		return col - 1;
    	else
    		return numColHeaders + col;
    }
}
