package edu.mayo.pipes.util.metadata;

/**
 * This class is used in the constructor to the HistoryInPipe
 * @author Michael Meiners (m054457)
 * Date created: Aug 1, 2013
 */
public class Metadata {
	public static enum CmdType { Query, Drill, ToTJson, Tool };
	
	private CmdType mCmdType;

    private String operaton;
	private String  mCatalogPath;
	private int     mColNum;
	private String[] mDrillPaths;
	
	/** Use for bior_vcf_to_tjson and other to_tjson commands ; basically input functions*/
	public Metadata(CmdType cmdType) {
		
	}

	/** Use for bior_overlap, bior_same_variant, bior_lookup ; basically all query functions that use a catalog */
	public Metadata(CmdType cmdType, String catalogPath) {
		
	}

	/** Use for bior_drill ; use with drill functions */
	public Metadata(CmdType cmdType, int colNum, String... drillPaths) {
		
	}

    public CmdType getmCmdType() {
        return mCmdType;
    }

    public void setmCmdType(CmdType mCmdType) {
        this.mCmdType = mCmdType;
    }

    public String getmCatalogPath() {
        return mCatalogPath;
    }

    public void setmCatalogPath(String mCatalogPath) {
        this.mCatalogPath = mCatalogPath;
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

    public String getOperaton() {
        return operaton;
    }

    public void setOperaton(String operaton) {
        this.operaton = operaton;
    }
}
