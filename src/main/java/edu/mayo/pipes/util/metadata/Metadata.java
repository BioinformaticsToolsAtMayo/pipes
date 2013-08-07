package edu.mayo.pipes.util.metadata;

/**
 * This class is used in the constructor to the HistoryInPipe
 * @author Michael Meiners (m054457)
 * Date created: Aug 1, 2013
 */
public class Metadata {
	public static enum CmdType { Query, Drill, ToTJson, Tool };
	
	private CmdType mCmdType;

    private String operator;
	private String  mFullCanonicalPath;
    private String datasourcepath;
    private String columnspath;
	private int     mColNum =-1;
	private String[] mDrillPaths;
    private boolean keepJSON = false; //used for drill

	
	/** Use for bior_vcf_to_tjson and other to_tjson commands ; basically input functions*/
	public Metadata(String operator) {
		this.mCmdType = CmdType.ToTJson;
        this.operator = operator;
	}

	/** Use for bior_overlap, bior_same_variant, bior_lookup ; basically all query functions that use a catalog */
	public Metadata(String mFullCanonicalPath, String operator) {
        this.mCmdType = CmdType.Query;
        this.operator = operator;
        this.mFullCanonicalPath = mFullCanonicalPath;
	}

	/** Use for bior_drill ; use with drill functions */
	public Metadata(int colNum, String operator, boolean keepJSON, String... drillPaths ) {
        mDrillPaths = drillPaths;
        this.mCmdType = CmdType.Drill;
        this.operator = operator;
        this.keepJSON = keepJSON;
        this.mColNum = colNum;
	}

    /** use for any tool */
    public Metadata(String fullCanonicalPathDataSourceProps, String fullCanonicalPathColumnProps,  String operator) {
        this.mCmdType = CmdType.Tool;
        this.columnspath = fullCanonicalPathColumnProps;
        this.datasourcepath = fullCanonicalPathDataSourceProps;
        this.operator = operator;
    }

    public CmdType getmCmdType() {
        return mCmdType;
    }

    public void setmCmdType(CmdType mCmdType) {
        this.mCmdType = mCmdType;
    }


    public String getmFullCanonicalPath() {
        return mFullCanonicalPath;
    }

    public void setmFullCanonicalPath(String mFullCanonicalPath) {
        this.mFullCanonicalPath = mFullCanonicalPath;
    }

    public int getmColNum() {
        return mColNum;
    }

    public void setmColNum(int mColNum) {
        this.mColNum = mColNum;
    }

    public String[] getmDrillPaths() {
        return mDrillPaths;
    }

    public void setmDrillPaths(String[] mDrillPaths) {
        this.mDrillPaths = mDrillPaths;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public boolean isKeepJSON() {
        return keepJSON;
    }

    public void setKeepJSON(boolean keepJSON) {
        this.keepJSON = keepJSON;
    }

    public String getDatasourcepath() {
        return datasourcepath;
    }

    public void setDatasourcepath(String datasourcepath) {
        this.datasourcepath = datasourcepath;
    }

    public String getColumnspath() {
        return columnspath;
    }

    public void setColumnspath(String columnspath) {
        this.columnspath = columnspath;
    }
}
