package edu.mayo.pipes.util.index;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class FindIndex {
    
	/**
	 * From a specified set of Ids, find all id-filePosition pairs in the index
	 * @param idsToFind
	 * @param isKeyInteger
	 * @param dbConn
	 * @return
	 * @throws SQLException
	 */
	public HashMap<String, List<Long>> find(List<String> idsToFind, boolean isKeyInteger, Connection dbConn) throws SQLException {
		final String SQL = "SELECT FilePos FROM Indexer WHERE Key = ?";
		PreparedStatement stmt = dbConn.prepareStatement(SQL);

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
			
			if(isKeyInteger)
				stmt.setLong(1, Long.valueOf(id));
			else
				stmt.setString(1, id);
			
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
	public List<Long> find(String idToFind, boolean isKeyAnInteger, Connection dbConn) throws SQLException {
		final String SQL = "SELECT FilePos FROM Indexer WHERE Key = ?";
		PreparedStatement stmt = dbConn.prepareStatement(SQL);
		List<Long> positions = new ArrayList<Long>();
		
		if(isKeyAnInteger)
			stmt.setLong(1, Long.valueOf(idToFind));
		else
			stmt.setString(1, idToFind);
				
		ResultSet rs = stmt.executeQuery();
		while(rs.next()) {
			Long pos = rs.getLong("FilePos");
			positions.add(pos);
		}
		rs.close();		
		stmt.close();

		return positions;
	}		

}
