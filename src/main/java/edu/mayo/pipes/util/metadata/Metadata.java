package edu.mayo.pipes.util.metadata;

import edu.mayo.pipes.util.FieldSpecification;

/**
 * This class is used in the constructor to the HistoryInPipe
 * @author Michael Meiners (m054457)
 * Date created: Aug 1, 2013
 */
public class Metadata {
	public static enum CmdType { Query, Drill, ToTJson, Tool, Annotate, Compress };

	private CmdType mCmdType;

    private String mDataSourceCanonicalPath;
    private String mColumnCanonicalPath;
    private String 	mOperator;
	private String  mFullCanonicalPath;
    private String 	mDatasourcePath;
    private String 	mColumnsPath;
	private int     mColNum =-1;
	private String[] mDrillPaths; // Used for drill
    private boolean mKeepJSON = false; //used for drill
    private String[] mNewColNamesForDrillPaths; // Used for bior_annotate
    
    private String  mDelimiter; // Used by bior_compress to note how the values are separated
    private String  mEscDelimiter; // Used by bior_compress as the escaped delimiter that will replace any naturally occurrences of the delimiter character in the value
	private FieldSpecification mCompressFieldSpecs; // bior_compress must know which columns to compress so it can change the "Number" field to "."

	/** Use for bior_vcf_to_tjson and other to_tjson commands ; basically input functions*/
	public Metadata(String operator) {
		this.mCmdType = CmdType.ToTJson;
        this.mOperator = operator;
	}

	/** Use for bior_overlap, bior_same_variant, bior_lookup ; basically all query functions that use a catalog */
	public Metadata(String mFullCanonicalPath, String operator) {
        this.mCmdType = CmdType.Query;
        this.mOperator = operator;
        this.mFullCanonicalPath = mFullCanonicalPath;
	}

	/** Use for bior_drill ; use with drill functions */
	public Metadata(int colNum, String operator, boolean keepJSON, String... drillPaths ) {
        mDrillPaths = drillPaths;
        this.mCmdType = CmdType.Drill;
        this.mOperator = operator;
        this.mKeepJSON = keepJSON;
        this.mColNum = colNum;
	}

    /** use for any tool */
    public Metadata(String fullCanonicalPathDataSourceProps, String fullCanonicalPathColumnProps,  String operator) {
        this.mCmdType = CmdType.Tool;
        this.mColumnsPath = fullCanonicalPathColumnProps;
        this.mDatasourcePath = fullCanonicalPathDataSourceProps;
        this.mOperator = operator;
    }
    
	/** Use for bior_annotate, where you want to remove a bunch of columns and add your own in.
	 *  To just remove a column (such as a drilled column which was used in the middle of bior_annotate)
	 *  without adding another, just pass in null for the colNameToAdd 
	 *  @param operator   The script name that was run (such as "bior_annotate")
	 *  @param colNamesToAdd   A String array of the column names to add (this will be user-specified columns, NOT the drill paths)
	 *  @param fullCanonicalPathCatalog   The full path to the catalog from which the data came
	 **/
	public Metadata(String operator, String fullCanonicalPathCatalog, String[] colNamesToAdd, String[] drillPaths) {
        
		this.mOperator = operator;
        this.mFullCanonicalPath = fullCanonicalPathCatalog;
        this.mNewColNamesForDrillPaths = colNamesToAdd;
        this.mDrillPaths = drillPaths;
        this.mCmdType = CmdType.Annotate;
	}

	/** Use for bior_annotate when drill path is on tool JSON column, where you want to remove a bunch of columns and add your own in.
	 *  To just remove a column (such as a drilled column which was used in the middle of bior_annotate)
	 *  without adding another, just pass in null for the colNameToAdd 
	 *  @param operator   The script name that was run (such as "bior_annotate")
	 *  @param colNamesToAdd   A String array of the column names to add (this will be user-specified columns, NOT the drill paths)
	 *  @param fullCanonicalPathCatalog   The full path to the catalog from which the data came
	 **/

    public Metadata(String operator, String dataSourcecanonicalPath, String columnsCanonicalPath,
			String[] colNamesToAdd, String[] drillPaths) {
    	this.mOperator = operator;
        this.mDataSourceCanonicalPath = dataSourcecanonicalPath;
        this.mColumnCanonicalPath =  columnsCanonicalPath;
        this.mNewColNamesForDrillPaths = colNamesToAdd;
        this.mDrillPaths = drillPaths;
        this.mCmdType = CmdType.Annotate;
		
	}

    /** Use for bior_compress which will need to modify existing ##BIOR lines 
     *  and change the Number field, and add a Delimiter field 
     * @param delimiter  The separator of multiple values
     * @param escapedDelimiter  The substitute string to be used whenever the delimiter string is encountered in the value 
     *                         (for example: say you have 3 values: "1", "2|3", "4".  If the delimiter was "|" and the escaped delimiter was "\|", 
     *                         then the compressed value will be "1|2\|3|4" because the delimiter character already occurred within the value. 
     * @param colsToCompress  0-based indexes that point to the columns that are to be compressed
     */
    public Metadata(String delimiter, String escapedDelimiter, FieldSpecification fieldSpecs) {
    	mDelimiter = delimiter;
    	mEscDelimiter = escapedDelimiter;
    	mCompressFieldSpecs = fieldSpecs;
    	this.mCmdType = CmdType.Compress;
    }
    
	public CmdType getCmdType() {
        return mCmdType;
    }

    public void setCmdType(CmdType cmdType) {
        this.mCmdType = cmdType;
    }

    public String getFullCanonicalPath() {
        return mFullCanonicalPath;
    }

    public void setFullCanonicalPath(String fullCanonicalPath) {
        this.mFullCanonicalPath = fullCanonicalPath;
    }

    public int getColNum() {
        return mColNum;
    }

    public void setColNum(int colNum) {
        this.mColNum = colNum;
    }

    public String[] getDrillPaths() {
        return mDrillPaths;
    }

    public void setDrillPaths(String[] drillPaths) {
        this.mDrillPaths = drillPaths;
    }

    public String getOperator() {
        return mOperator;
    }

    public void setOperator(String operator) {
        this.mOperator = operator;
    }

    public boolean isKeepJSON() {
        return mKeepJSON;
    }

    public void setKeepJSON(boolean keepJSON) {
        this.mKeepJSON = keepJSON;
    }

    public String getDatasourcePath() {
        return mDatasourcePath;
    }

    public void setDatasourcePath(String datasourcePath) {
        this.mDatasourcePath = datasourcePath;
    }

    public String getColumnsPath() {
        return mColumnsPath;
    }

    public void setColumnsPath(String columnsPath) {
        this.mColumnsPath = columnsPath;
    }
    
	public String[] getNewColNamesForDrillPaths() {
		return mNewColNamesForDrillPaths;
	}

	public void setNewColNamesForDrillPaths(String[] newColNamesForDrillPaths) {
		this.mNewColNamesForDrillPaths = newColNamesForDrillPaths;
	}

	public String getDataSourceCanonicalPath() {
		return mDataSourceCanonicalPath;
	}

	public void setDataSourceCanonicalPath(String dataSourceCanonicalPath) {
		this.mDataSourceCanonicalPath = dataSourceCanonicalPath;
	}

	public String getColumnCanonicalPath() {
		return mColumnCanonicalPath;
	}

	public void setColumnCanonicalPath(String columnCanonicalPath) {
		this.mColumnCanonicalPath = columnCanonicalPath;
	}
	
    public String getDelimiter() {
		return mDelimiter;
	}

	public void setDelimiter(String delimiter) {
		this.mDelimiter = delimiter;
	}

    public String getEscapedDelimiter() {
		return mEscDelimiter;
	}

	public void setEscapedDelimiter(String escapedDelimiter) {
		this.mEscDelimiter = escapedDelimiter;
	}

	public FieldSpecification getCompressFieldSpecs() {
		return mCompressFieldSpecs;
	}

	public void setCompressFieldSpecs(FieldSpecification fieldSpecs) {
		this.mCompressFieldSpecs = fieldSpecs;
	}


}
