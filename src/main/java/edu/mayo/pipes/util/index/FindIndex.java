package edu.mayo.pipes.util.index;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import edu.mayo.pipes.JSON.lookup.lookupUtils.IndexUtils;

public class FindIndex {
    
	private Connection mDbConn;
	private boolean mIsKeyAnInteger = false;
	// The new index table has a KeyUpper column so we can do case-insensitive searches
	private boolean mIsKeyUpperColumnExists = false;
	
	public FindIndex() {
	}
	
	public FindIndex(Connection dbConn) {
		mDbConn = dbConn;
		mIsKeyAnInteger = IndexUtils.isKeyAnInteger(dbConn);
		mIsKeyUpperColumnExists = isKeyUpperColExists(dbConn);
	}
		
	/**
	 * From a specified set of Ids, find all id-filePosition pairs in the index
	 * @param idsToFind
	 * @param isKeyInteger
	 * @param dbConn
	 * @return
	 * @throws SQLException
	 */
	public HashMap<String, List<Long>> find(List<String> idsToFind) throws SQLException {
		final String SQL = "SELECT FilePos FROM Indexer WHERE Key = ?";
		PreparedStatement stmt = mDbConn.prepareStatement(SQL);

		HashMap<String,List<Long>> key2posMap = new HashMap<String,List<Long>>();
		int count = 0;
		
		// Remove any duplicate ids by assigning to a HashSet
		double start = System.currentTimeMillis();
		Set<String> idSet = new HashSet<String>(idsToFind);
		Iterator<String> it = idSet.iterator();		
		double end = System.currentTimeMillis();		
		//System.out.println("Time to create set from list: " + (end-start)/1000.0);
		//System.out.println("Set size: " + idSet.size());
		
		//IndexUtils utils = new IndexUtils();
		long maxMem = 0;
		while(it.hasNext()) {
			String id = it.next();
			count++;
			
			/** Commenting for now.. enable if required.
			if(count % 100000 == 0 ) {
				//System.out.println(".");
				double now = System.currentTimeMillis();
				int numPerSec = (int)(count/((now-end)/1000.0));
				long mem = utils.getMemoryUseMB();
				if(mem > maxMem)
					maxMem = mem;
				System.out.println(count + "\t #/sec: " + numPerSec + "\t Est time: " + ((idSet.size()-count)/numPerSec) + " s  " + mem + "MB");
			}*/
			
			List<Long> positions = key2posMap.get(id);
			if(positions == null) {
				positions = new ArrayList<Long>();
				key2posMap.put(id, positions);
			}
			
			if(mIsKeyAnInteger)
				stmt.setLong(1, Long.valueOf(id));
			else
				stmt.setString(1, id.toUpperCase());
			
			ResultSet rs = stmt.executeQuery();
			while(rs.next()) {
				Long pos = rs.getLong("FilePos");
				positions.add(pos);
			}
			rs.close();
		}
		//System.out.println("Max memory: " + maxMem);
		stmt.close();

		return key2posMap;
	}		
	

	/**
	 * Given an Id, find a list of positions within the Bgzip file that correspond to that Id 
	 * @param idToFind
	 * @param isKeyAnInteger
	 * @param dbConn
	 * @return
	 * @throws SQLException
	 */
	public LinkedList<Long> find(String idToFind) throws SQLException {
		final String SQL = mIsKeyUpperColumnExists
				?  "SELECT FilePos FROM Indexer WHERE KeyUpper = ?"
				:  "SELECT FilePos FROM Indexer WHERE Key = ?";
		
		PreparedStatement stmt = mDbConn.prepareStatement(SQL);
		ResultSet rs = null;
		LinkedList<Long> positions = new LinkedList<Long>();
		
		try {
			if(mIsKeyAnInteger) {
				// If the key is an integer but the idToFind is a string, there will be no match
				// so return an empty list of positions
				if( ! IndexUtils.isInteger(idToFind) )
					return positions;
				stmt.setLong(1, Long.valueOf(idToFind));
			}
			else
				stmt.setString(1, mIsKeyUpperColumnExists ? idToFind.toUpperCase() : idToFind);
					
			rs = stmt.executeQuery();
			while(rs.next()) {
				Long pos = rs.getLong("FilePos");
				positions.add(pos);
			}
		} catch (NumberFormatException nfe) {
			Logger.getLogger(FindIndex.class.getName()).log(Level.DEBUG, "Invalid search ID. ID needs to be a number.", nfe);			
		} catch (Exception ex) {
			throw new SQLException("Exception in FindIndex.find(idToFind). " + ex.getMessage());
		} finally {
			if( rs != null )
				rs.close();
			if( stmt != null )
				stmt.close();
		}
		
		return positions;
	}
	
	private boolean isKeyUpperColExists(Connection dbConn) {
		Statement stmt = null;
		ResultSet rs = null;
		boolean isExists = false;
		try {
			stmt = dbConn.createStatement();
			rs = stmt.executeQuery("SELECT KeyUpper FROM Indexer");
			isExists = true;
		} catch(Exception e) {	}
		finally {
			try {
				if(rs != null)
					rs.close();
			}catch(Exception e) {}
			try {
				if(stmt != null) 
					stmt.close();
			}catch(Exception e) {}
		}
		return isExists;
	}

}
