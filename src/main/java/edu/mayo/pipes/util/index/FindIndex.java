package edu.mayo.pipes.util.index;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
	private boolean mIsKeyCaseSensitive = false;
	
	public FindIndex(Connection dbConn, boolean isKeyCaseSensitive) {
		mDbConn = dbConn;
		mIsKeyAnInteger = IndexUtils.isKeyAnInteger(dbConn);
		mIsKeyCaseSensitive = isKeyCaseSensitive;
	}
	
	public FindIndex(Connection dbConn) {
		this(dbConn, false);
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
		final String SQL = "SELECT Key,FilePos FROM Indexer WHERE Key = ?";
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
			
			List<Long> positions = key2posMap.get(id);
			if(positions == null) {
				positions = new ArrayList<Long>();
				key2posMap.put(id, positions);
			}
			
			if(mIsKeyAnInteger)
				stmt.setLong(1, Long.valueOf(id));
			else
				stmt.setString(1, id);
			
			ResultSet rs = stmt.executeQuery();
			while(rs.next()) {
				Long pos = rs.getLong("FilePos");
				Object key = rs.getObject("Key");
				// Don't add the position if the key is NOT an integer AND it is case sensitive AND it does not equal exactly
				if( ! mIsKeyAnInteger && mIsKeyCaseSensitive && ! id.equals((String)key) )
					continue;
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
		final String SQL = "SELECT Key,FilePos FROM Indexer WHERE Key = ?";
		
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
				stmt.setString(1, idToFind);
					
			rs = stmt.executeQuery();
			while(rs.next()) {
				Long pos = rs.getLong("FilePos");
				Object key = rs.getObject("Key");
				// Don't add the position if the key is NOT an integer AND it is case sensitive AND it does not equal exactly
				if( ! mIsKeyAnInteger && mIsKeyCaseSensitive && ! idToFind.equals((String)key) )
					continue;
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


}
