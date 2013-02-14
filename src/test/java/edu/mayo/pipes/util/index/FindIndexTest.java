package edu.mayo.pipes.util.index;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import edu.mayo.pipes.util.CatalogUtils;
import edu.mayo.pipes.JSON.lookup.lookupUtils.IndexUtils;

public class FindIndexTest {
	
	/**
	 * TEST ID's
	 * Duplicates: GeneIDs: 438, 715 -- 2 of each
	 * Single: GeneIds: 1, 2, 3
	 * Not Found: 4, 5, 6
	 * 
	 */
	@Test
	public void testFindIndex() throws Exception {
		System.out.println("Testing LookupPipeTest.testFindIndex()..");
	
		String idTwoRows = "715"; //gene-id - a duplicate (2 rows)
		String idOneRow  = "1";  //GeneID - only 1
		String idZeroRows= "4";
		
		String databaseFile = "src/test/resources/testData/tabix/index/genes.GeneID.idx.h2.db";
		H2Connection h2 = new H2Connection(databaseFile);
		Connection dbConn = h2.getConn();
		
		// Find index
		FindIndex findIndex = new FindIndex();		
		List<Long> pos0rows = findIndex.find(idZeroRows, true, dbConn);		
		List<Long> pos1row  = findIndex.find(idOneRow,   true, dbConn);		
		List<Long> pos2rows = findIndex.find(idTwoRows,  true, dbConn);		
		
		assertEquals(Arrays.asList(), pos0rows);
		assertEquals(Arrays.asList(72805499555L), pos1row);
		assertEquals(Arrays.asList(28950243673L, 28950243981L), pos2rows);

		dbConn.close();		
		dbConn = null;
		h2 = null;
	}	

	@Test
	public void testFindIndex_IdAsString() throws Exception {
		System.out.println("Testing LookupPipeTest.testFindIndex_IdAsString()..");
	
		String idOneRow  = "1";  //GeneID - only 1
		
		String databaseFile = "src/test/resources/testData/tabix/index/genes.GeneID.idx.h2.db";
		H2Connection h2 = new H2Connection(databaseFile);
		Connection dbConn = h2.getConn();
		
		// Find index
		FindIndex findIndex = new FindIndex();		
		List<Long> pos1row  = findIndex.find(idOneRow, dbConn);		
		
		assertEquals(Arrays.asList(72805499555L), pos1row);

		dbConn.close();		
		dbConn = null;
		h2 = null;
	}	

}