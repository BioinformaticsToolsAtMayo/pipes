package edu.mayo.pipes.util.index;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class H2Connection {
	
	Connection conn = null;
	
	public H2Connection(File databaseFile) {
		try {
			this.conn = getConnection(databaseFile);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
    private Connection getConnection(File databaseFile) throws ClassNotFoundException, SQLException, IOException {
    	Class.forName("org.h2.Driver");
		String dbPath = databaseFile.getCanonicalPath().replace(".h2.db", "");
		System.out.println("Database path: " + dbPath);
        String url = "jdbc:h2:file:" + dbPath + ";FILE_LOCK=SERIALIZED";
        //double start = System.currentTimeMillis();
        Connection conn = DriverManager.getConnection(url, "sa", "");
        //double end = System.currentTimeMillis();
        //System.out.println("Time to connect to database: " + (end-start)/1000.0);
        
        return conn;
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
    
}
