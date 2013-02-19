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
		init(new File(databasePath));
	}
	
	public H2Connection(File databaseFile) {
		init(databaseFile);
	}
  
	private void init(File databaseFile){
		try {
			this.conn = getConnection(databaseFile);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
    public Connection getConn() {
		return this.conn;
	}

	private Connection getConnection(File databaseFile) throws ClassNotFoundException, SQLException, IOException {
    	Class.forName("org.h2.Driver");
		String dbPath = databaseFile.getCanonicalPath().replace(".h2.db", "");
		//System.out.println("Database path: " + dbPath);
        String url = "jdbc:h2:file:" + dbPath + ";FILE_LOCK=SERIALIZED";
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
