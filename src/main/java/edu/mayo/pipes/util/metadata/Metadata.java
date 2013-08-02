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
	private int     mColNum;
	private String[] mDrillPaths;
    private boolean keepJSON = false; //used for drill
	
	/** Use for bior_vcf_to_tjson and other to_tjson commands ; basically input functions*/
	public Metadata(CmdType cmdType, String operator) {
		this.mCmdType = cmdType;
        this.operator = operator;
	}

	/** Use for bior_overlap, bior_same_variant, bior_lookup ; basically all query functions that use a catalog */
	public Metadata(CmdType cmdType, String mFullCanonicalPath, String operator) {
        this.mCmdType = cmdType;
        this.operator = operator;
        this.mFullCanonicalPath = mFullCanonicalPath;
	}

	/** Use for bior_drill ; use with drill functions */
	public Metadata(CmdType cmdType, int colNum, String operator, boolean keepJSON, String... drillPaths ) {
        mDrillPaths = drillPaths;
        this.mCmdType = cmdType;
        this.operator = operator;
        this.keepJSON = keepJSON;
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
}
