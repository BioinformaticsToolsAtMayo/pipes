package edu.mayo.pipes.history;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.util.StringUtils;
import edu.mayo.pipes.util.metadata.AddMetadataLines;
import edu.mayo.pipes.util.metadata.Metadata;

/**
 * Processes incoming String data and deserializes it into a History object.
 * 
 * @author duffp
 * dquest
 *
 */
public class HistoryInPipe extends AbstractPipe<String, History> {

    ArrayList<Metadata> metadata = new ArrayList<Metadata>();

    /**
     * Make a new HistoryInPipe with metadata operations pending
     * metadata operations allow the ## lines to be modified
     * on the first pipe (HistoryInPipe) in the pipeline
     * @param md
     */
    public HistoryInPipe(Metadata md){
         metadata.add(md);
    }

    public HistoryInPipe(ArrayList<Metadata> md){
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
			int numCols = line.split(COL_DELIMITER).length;
			
			initializeMetaData(history, headerRows, numCols);
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
    public void insertBIORLines(History h){
        if(this.metadata.size() < 1){
            //do nothing we don't have to add header lines
        }else {
            HistoryMetaData hMeta = History.getMetaData();
            for(int i=0; i< this.metadata.size(); i++) {
                Metadata md = metadata.get(i);
                //type = ToTJson
                if(md.getmCmdType().toString().equalsIgnoreCase(Metadata.CmdType.ToTJson.toString())){
                    amdl.constructToJsonLine(h, md.getOperator());
                    ColumnMetaData cmd = new ColumnMetaData(md.getOperator());
                    hMeta.getColumns().add(cmd);
                }
                //type = Drill
                else if(md.getmCmdType().toString().equalsIgnoreCase(Metadata.CmdType.Drill.toString())){
                     //String col = amdl.constructDrillLine(h, md.getmColNum(), md.getmDrillPaths());
                }
                //type = Query
                else if(md.getmCmdType().toString().equalsIgnoreCase(Metadata.CmdType.Query.toString())){
                    try {
                        String col = amdl.constructQueryLine(h, md.getmFullCanonicalPath(), md.getOperator());
                        ColumnMetaData cmd = new ColumnMetaData(col);
                        hMeta.getColumns().add(cmd);
                    } catch (IOException e) {
                        //if there does not exist a datasource.properties for a given catalog, then we need to modify
                        //the column but we can't add a header.  Note, this is not an error condition, but an expected
                        //code path that needs to be tested.
                        String col = amdl.constructQueryLineOnNoDatasourceProperties(h, md.getmFullCanonicalPath(), md.getOperator());
                        ColumnMetaData cmd = new ColumnMetaData(col);
                        hMeta.getColumns().add(cmd);
                    }
                }
                //type = Tool
                else if(md.getmCmdType().toString().equalsIgnoreCase(Metadata.CmdType.Tool.toString())){
                    try {
                        //if the file is not there, then we can't change the metadata to include the tool
                        String shortname = amdl.constructToolLine(h,md.getOperator(),md.getmFullCanonicalPath());
                        ColumnMetaData cmd = new ColumnMetaData("bior."+shortname);
                        hMeta.getColumns().add(cmd);
                    } catch (IOException e) {
                       throw new RuntimeException("Stupid Developer! You need to put the path of the VEP/SNPEFF/Tool into your project and pass it to the metadata object\n  Look at HistoryInPipeTest for an example.\n CurrentPath: " + md.getmFullCanonicalPath());
                    }
                }
            }

        }
    }
}
