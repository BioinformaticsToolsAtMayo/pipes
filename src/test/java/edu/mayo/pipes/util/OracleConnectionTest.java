/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.util;

import java.sql.*;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author dquest
 */
public class OracleConnectionTest {
    
    public OracleConnectionTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of connect method, of class OracleConnection.
     */
    //@Test
    public void testConnect() throws SQLException {
        System.out.println("connect");
        OracleConnection connection = new OracleConnection();
        Connection con = connection.connect(true);
        
        connection.createSchema(con);
        String sql = "INSERT INTO XREF (pkeyid, id, nspace, version, symbol, type, originalDataSource)" +
                     " VALUES (XREFSeq.nextval, 'ps1234', 'PFAM', 'Today', 'ABC123', 'protein', 'original')";
        String sql2 = "INSERT INTO XREF (pkeyid, id, nspace, version, symbol, type, originalDataSource)" +
                     " VALUES (XREFSeq.nextval, 'ps1235', 'PFAM', 'Today', 'ABC125', 'protein', 'original')";
        Statement stmt = con.createStatement();
        stmt.executeUpdate(sql);
        stmt.executeUpdate(sql2);
        ResultSet results = stmt.executeQuery("select count(*) from xref");
        int result = 0;
        while (results.next()){
            ResultSetMetaData metaData = results.getMetaData();
            System.out.println(metaData.getColumnName(1));
            result = results.getInt(1);
        }
        
        connection.dropSchema(con);
        con.close();
        assertEquals(2, result);
    }
    
    @Test
    public void testConnectionParameters() throws SQLException {
    	OracleConnection connection = new OracleConnection();
        Connection con = connection.connect(false); //test actual oracle connection parameters
        assertTrue(!con.isClosed());
    }
}
