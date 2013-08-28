package edu.mayo.pipes.util.metadata;

/**
 * This class is used in the constructor to the HistoryInPipe
 * @author Michael Meiners (m054457)
 * Date created: Aug 1, 2013
 */
public class Metadata {
	public static enum CmdType { Query, Drill, ToTJson, Tool, Annotate };

	private CmdType mCmdType;

    private String mDataSourceCanonicalPath;
    private String mColumnCanonicalPath;
    private String 	mOperator;
	private String  mFullCanonicalPath;
    private String 	mDatasourcePath;
    private String 	mColumnsPath;
	private int     mColNum =-1;
	private String[] mDrillPaths;
    private boolean mKeepJSON = false; //used for drill
    private String[] mNewColNamesForDrillPaths; // Used for bior_annotate

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

	public String getmDataSourceCanonicalPath() {
		return mDataSourceCanonicalPath;
	}

	public void setmDataSourceCanonicalPath(String mDataSourceCanonicalPath) {
		this.mDataSourceCanonicalPath = mDataSourceCanonicalPath;
	}

	public String getmColumnCanonicalPath() {
		return mColumnCanonicalPath;
	}

	public void setmColumnCanonicalPath(String mColumnCanonicalPath) {
		this.mColumnCanonicalPath = mColumnCanonicalPath;
	}


}
