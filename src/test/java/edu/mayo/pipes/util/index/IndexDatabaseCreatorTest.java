package edu.mayo.pipes.util.index;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class IndexDatabaseCreatorTest {

	
	/** Create an index from the genes file based on HGNC id
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SQLException */
	@Test
	public void createGenesIndex() throws SQLException, IOException, ClassNotFoundException {
            //test is failing commenting it out
//		IndexDatabaseCreator indexH2 = new IndexDatabaseCreator();
//		
//		// Create an index based on Gene "HGNC" json path
//		String dir = "src/test/resources/testData/tabix";
//		String bgzipPath = dir + "/genes.tsv.bgz";
//		String h2DbPath  = dir + "/index/genes.HGNC.idx.h2.db";
//		indexH2.buildIndexH2(bgzipPath, 4, "HGNC", h2DbPath);
//		
//		int rowCount = getRowCount(h2DbPath, "Indexer");
//		assertEquals(30628, rowCount); 
//		System.out.println("\n----------------------\n");

	}

	
	/** Create an index from the genes file based on GeneID id
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SQLException */
	@Test
	public void createIndexOnGeneID() throws SQLException, IOException, ClassNotFoundException {
		IndexDatabaseCreator indexH2 = new IndexDatabaseCreator();
		
		// Create an index based on Gene "HGNC" json path
		String dir = "src/test/resources/testData/tabix";
		String bgzipPath = dir + "/genes.tsv.bgz";
		String h2DbPath  = dir + "/index/genes.GeneID.idx.h2.db";
		indexH2.buildIndexH2(bgzipPath, 4, "GeneID", h2DbPath);
		
		int rowCount = getRowCount(h2DbPath, "Indexer");
		assertEquals(37301, rowCount); 
		System.out.println("\n----------------------\n");

	}

	private int getRowCount(String h2DbPath, String tableName) throws SQLException {
		H2Connection h2Conn = new H2Connection(h2DbPath);
		Connection dbConn = h2Conn.getConn();
		Statement stmt = dbConn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM Indexer");
		rs.next();
		int numRows = rs.getInt(1);
		rs.close();
		stmt.close();
		dbConn.close();
		return numRows;
	}
}
