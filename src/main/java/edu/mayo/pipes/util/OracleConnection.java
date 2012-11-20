/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.util;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author dquest
 */
public class OracleConnection {
    

    private String biorddl;
    private String biordropddl;
    public Connection connect(boolean testing){  
        Connection connection = null;
        try {  
            SystemProperties sysprop = new SystemProperties();
            biorddl = sysprop.get("bior.warehouse.ddl");
            biordropddl = sysprop.get("bior.warehouse.dropddl");
            if(testing == true){
                Class.forName("org.h2.Driver");
                connection = DriverManager.
                getConnection("jdbc:h2:mem:test;MODE=Oracle");//"jdbc:h2:~/test" 
                //conn.close();
            }
            else { //production
                //url example:
                //URL jdbc:oracle:thin:@bior-ora-dev.cmnzfcwmkf1f.us-east-1.rds.amazonaws.com:1521:biordev
                // Load the JDBC driver
                String driverName = "oracle.jdbc.driver.OracleDriver";
                Class.forName(driverName);
                // Create a connection to the database           
                //String serverName = sysprop.get("bior.warehouse.hostname");
                //String portNumber = sysprop.get("bior.warehouse.port");
                //String sid = sysprop.get("bior.warehouse.name");
                //String url = "jdbc:oracle:thin:@" + serverName + ":" + portNumber + "/" + sid;
                String url = sysprop.get("bior.warehouse.oracleurl");
                String username = sysprop.get("bior.warehouse.user");
                String password = sysprop.get("bior.warehouse.password");                
                connection = DriverManager.getConnection(url, username, password);
            }
            return connection;
        } catch (IOException ex) {
            Logger.getLogger(OracleConnection.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException e) {
            // Could not find the database driver
        } catch (SQLException e) {
            // Could not connect to the database
        }
        return null;
    }


    public void dropSchema(Connection connection) throws SQLException {
        String sql = "";
        CatPipe cat = new CatPipe();
        PrintPipe print = new PrintPipe();
        Pipe p = new Pipeline(cat);
        p.setStarts(Arrays.asList(biordropddl));
        while(p.hasNext()){ sql += (String) p.next(); }  
        Statement stmt = connection.createStatement();
        stmt.execute(sql);
    }

    public void createSchema(Connection connection) throws SQLException {
        String sql = "";
        CatPipe cat = new CatPipe();
        PrintPipe print = new PrintPipe();
        Pipe p = new Pipeline(cat);
        p.setStarts(Arrays.asList(biorddl));
        while(p.hasNext()){ sql += (String) p.next(); }
        Statement stmt = connection.createStatement();
        stmt.execute(sql);
        return;
    }


}
