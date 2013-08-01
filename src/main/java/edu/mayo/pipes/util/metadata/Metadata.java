package edu.mayo.pipes.util.metadata;

/**
 * @author Michael Meiners (m054457)
 * Date created: Aug 1, 2013
 */
public class Metadata {
	public enum CmdType { Query, Drill, ToTJson, Tool };
	
	private CmdType mCmdType;
	private String  mCatalogPath;
	private int     mColNum;
	private String[] mDrillPaths;
	
	/** Use for bior_vcf_to_tjson */
	public Metadata(CmdType cmdType) {
		
	}

	/** Use for bior_overlap, bior_same_variant */
	public Metadata(CmdType cmdType, String catalogPath) {
		
	}

	/** Use for bior_drill */
	public Metadata(CmdType cmdType, int colNum, String... drillPaths) {
		
	}

	
}
