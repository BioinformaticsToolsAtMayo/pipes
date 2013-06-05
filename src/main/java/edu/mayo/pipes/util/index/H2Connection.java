package edu.mayo.pipes.util.index;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class H2Connection {
	
	Connection conn = null;
        
	
	public H2Connection(String databasePath){
		init(new File(databasePath), true);
	}
	
	public H2Connection(String databasePath, boolean isWritable) {
		init(new File(databasePath), isWritable);		
	}
	
	public H2Connection(File databaseFile) {
		init(databaseFile, true);
	}
  
	private void init(File databaseFile, boolean isWritable){
		try {
			this.conn = getConnection(databaseFile, isWritable);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
    public Connection getConn() {
		return this.conn;
	}

	private Connection getConnection(File databaseFile, boolean isWritable) throws ClassNotFoundException, SQLException, IOException {
    	Class.forName("org.h2.Driver");
		String dbPath = databaseFile.getCanonicalPath().replace(".h2.db", "");
		//System.out.println("Database path: " + dbPath);
		// Allow multiple connections to the database (FILE_LOCK=SERIALIZED)
		// AND make the columns NOT case sensitive (IGNORECASE=TRUE)
        String url = isWritable 
        		?  "jdbc:h2:file:" + dbPath + ";IGNORECASE=TRUE;FILE_LOCK=SERIALIZED;"
        		:  "jdbc:h2:file:" + dbPath + ";IGNORECASE=TRUE;FILE_LOCK=NO;LOG=0;UNDO_LOG=0;LOCK_MODE=0;ACCESS_MODE_DATA=r;TRACE_LEVEL_FILE=0;TRACE_LEVEL_SYSTEM_OUT=0;";
        //double start = System.currentTimeMillis();
        Connection conn = DriverManager.getConnection(url, "sa", "");
        //double end = System.currentTimeMillis();
        //System.out.println("Time to connect to database: " + (end-start)/1000.0);
        
        return conn;
    }
    
	public List<String> getTables(Connection dbConn) throws SQLException {
		List<String> tableNames = new ArrayList<String>();
		String query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'TABLE'";
		Statement stmt = dbConn.createStatement();
		ResultSet rs = stmt.executeQuery(query);
		while(rs.next()) {
			tableNames.add(rs.getString("TABLE_NAME"));
		}
		rs.close();
		stmt.close();
		return tableNames;
	}
	
    public void createTable(boolean isKeyInteger, int maxKeyLength, Connection dbConn) throws SQLException {
        final String SQL = "CREATE TABLE Indexer " 
        		+ "("
        		+   (isKeyInteger ? "Key BIGINT," : "Key VARCHAR(" + maxKeyLength + "), ")
        		+   "FilePos BIGINT" 
        		+ ")";
        Statement stmt = dbConn.createStatement();
        stmt.execute(SQL);
        stmt.close();
	}
    
	public void createTableIndex(Connection dbConn) throws SQLException {
		 final String SQL = "CREATE INDEX keyIndex ON Indexer (Key);";
		 Statement stmt = dbConn.createStatement();
		 stmt.execute(SQL);
		 stmt.close();
	}    
}
