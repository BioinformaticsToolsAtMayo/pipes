package edu.mayo.pipes.util.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import edu.mayo.pipes.JSON.lookup.lookupUtils.IndexUtils;

public class IndexDatabaseCreator {
	/** Reads a bgzip catalog, getting a key from col and jsonPath, then creating an H2 database/index 
	 * @param bgzipPath  Full path to bgzip catalog file
	 * @param keyCol 1-based column where the json is located
	 * @param jsonPath  The json path to get the id that we will index (ex: "HGNC" for HGNC Id within the Genes catalog)
	 * @param isKeyAnInteger  Is the key an integer or a string
	 * @param outH2DbFile  The name of the H2 database that will be created
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException */ 
	public void buildIndexH2(String bgzipPath, int keyCol, String jsonPath, String outH2DbPath) throws SQLException, IOException, ClassNotFoundException {
		Connection dbConn = null;
		File tempTxtOut = null;
		try {
			System.out.println("-------------- Building Index --------------");
			
			// First remove the database file
			File h2DbFile = new File(outH2DbPath);
			if(h2DbFile.exists()) {
				System.out.println("Deleting file: " + h2DbFile.getCanonicalPath());
				h2DbFile.delete();
			}
			
		    // NOTE: Indexes are saved to a text file first to avoid the huge memory locking issue
		    // (this occurred when reading from a bgzip file and trying to load directly to memory or a database
		    // but saving to files worked ok).
			
			// Save indexes to text file
		    System.out.println("Saving indexes to temp text file...");
			//addZipIndexesToDb(bgzipFile, 3, false, "\t", dbConn);
		    tempTxtOut = new File(h2DbFile.getParentFile().getCanonicalPath() + "/tempIndex.txt");
		    IndexUtils indexUtils = new IndexUtils();
		    Properties props = indexUtils.zipIndexesToTextFile(new File(bgzipPath), "\t", keyCol, jsonPath, tempTxtOut);
	
		    // Throw exception if maxKeyLen is 0, because then it didn't index anything
		    int maxKeyLen = (Integer)(props.get(IndexUtils.IndexBuilderPropKeys.MaxKeyLen));
		    if( 0 == maxKeyLen ) {
		    	throw new IllegalArgumentException("There were no keys indexed!  Check your inputs and try again.");
		    }
		    
		    // Read all data from text and put into H2 database
		    H2Connection h2Conn = new H2Connection(h2DbFile);
		    dbConn = h2Conn.getConn();
		    System.out.println("Create database table...");
		    boolean isKeyAnInteger = (Boolean)(props.get(IndexUtils.IndexBuilderPropKeys.IsKeyColAnInt));
		    h2Conn.createTable(isKeyAnInteger, maxKeyLen, dbConn);
		    System.out.println("Add rows from text file to database...");
		    textIndexesToDb(dbConn, false, tempTxtOut);
		    countDatabaseRows(h2DbFile);
			System.out.println("Creating index on database...");
			h2Conn.createTableIndex(dbConn);
	
			// Print the database
			//printDatabase(h2DbFile, isKeyAnInteger);
		} finally {
			if(dbConn != null && ! dbConn.isClosed())
				dbConn.close();
			// Remove the temp text file
			tempTxtOut.delete();
		}
	}
		

	/** Prints all rows in the database */
	public static void printDatabase(File h2DbFile) throws SQLException, ClassNotFoundException, IOException {
		System.out.println("\n\nPrinting table Indexer...");
		System.out.println(getTableAsString(h2DbFile));
	}
	
	public static String getTableAsString(File h2DbFile) throws SQLException {
		ArrayList<ArrayList<String>> table = getDatabaseTable(h2DbFile);
		StringBuilder str = new StringBuilder();
		for(int i=0; i < table.size(); i++) {
			str.append( (i==0 ? "" : i+")") );
			for(String col : table.get(i)) 
				str.append("\t" + col);
			str.append("\n");
		}
		return str.toString();
	}
	
	/** Return database key-pos table (including header row) 
	 * @throws SQLException */
	public static ArrayList<ArrayList<String>> getDatabaseTable(File h2DbFile) throws SQLException {
		ArrayList<ArrayList<String>> rows = new ArrayList<ArrayList<String>>();
		Connection dbConn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			dbConn = new H2Connection(h2DbFile).getConn();
			stmt = dbConn.createStatement();
			rs = stmt.executeQuery("SELECT * FROM Indexer ORDER BY Key");
	    
			// Print the column names
			int numCols = rs.getMetaData().getColumnCount();
			ArrayList<String> headerRow = new ArrayList<String>();
			for(int i=1; i <= numCols; i++)
				headerRow.add(rs.getMetaData().getColumnName(i));
			rows.add(headerRow);
			
			while(rs.next()) {
				ArrayList<String> row = new ArrayList<String>();
		    	String key = "" + rs.getObject(1);
		    	Long pos = rs.getLong(2);
		    	row.add(key);
		    	row.add("" + pos);
		    	rows.add(row);
		    }
		} finally {
			if( rs != null )
				rs.close();
			if( stmt != null )
				stmt.close();
			if( dbConn != null )
				dbConn.close();
		}
		return rows;
	}

	public static int countDatabaseRows(File h2DbFile) throws SQLException, ClassNotFoundException, IOException {
		Connection dbConn = new H2Connection(h2DbFile).getConn();
	    Statement stmt = dbConn.createStatement();
	    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM Indexer");
	    int count = 0;
	    while(rs.next()) {
	    	count = rs.getInt(1);
	    }
    	//System.out.println("# of rows in database = " + count);
	    rs.close();
	    stmt.close();
	    dbConn.close();
	    return count;
	}

	private void textIndexesToDb(Connection dbConn, boolean isKeyInteger, File tmpTxt) throws NumberFormatException, SQLException, IOException {
		long numObjects = 0;
		long numBytesRead = 0;
		long MB = 1024L * 1024L;

		BufferedReader fin = new BufferedReader(new FileReader(tmpTxt));
		
		final String SQL = "INSERT INTO Indexer (Key, FilePos) VALUES (?, ?)";
		PreparedStatement stmt = dbConn.prepareStatement(SQL);
		dbConn.setAutoCommit(true);

		String line = null;
		while( (line = fin.readLine()) != null ) {
			numObjects++;
			String[] cols = line.split("\t");
			String key = cols[0];
			String pos = cols[1];
			if(isKeyInteger)
				stmt.setLong(1, Integer.valueOf(key));
			else
				stmt.setString(1, key);
			stmt.setLong(2, Long.valueOf(pos));
			stmt.execute();
			dbConn.commit();

			if( numObjects % 10000 == 0 ) {
				System.out.println(key + "    " + numObjects 
						+ ", avgBytesPerItem: " + (numBytesRead / numObjects) 
						+ ", MBs read: " + (numBytesRead/MB) + ", Mem (MBs): " + (new IndexUtils().getMemoryUse()/MB));
			}
		} while( line != null );

		fin.close();
		stmt.close();
		
		System.out.println("Num objects read: " + numObjects);
	}

}
