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

public class LookupPipeTest {
	
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
	public void testLinesByIndex() throws Exception {
		System.out.println("Testing LookupPipeTest.testLinexByIndex()..");
		
		IndexUtils utils = new IndexUtils();
		File bgzipFile = new File("src/test/resources/testData/tabix/genes.tsv.bgz");
		File queryResultTxt = new File("src/test/resources/testData/tmpOut/queryResults.lookup.fromDb.txt");
		
		final String EXPECTED_RESULTS="src/test/resources/testData/tabix/expected.results.lookup.txt";
		final String QUERY_RESULTS = "src/test/resources/testData/tmpOut/queryResults.lookup.fromDb.txt";
		
		// the last entry, after json string is the lookup-id. here i used hgnc-id
		String INPUT = "chr\t41177258\t41184058\t750\t{\"type\":\"gene\",\"chr\":\"17\",\"strand\":\"+\",\"minBP\":41177258,\"maxBP\":41184058,\"gene\":\"RND2\",\"gene_synonym\":\"ARHN; RHO7; RhoN\",\"note\":\"Rho family GTPase 2\",\"GeneID\":\"8153\",\"HPRD\":\"03332\",\"MIM\":\"601555\"}";
	
		// if the lookup if found, the related data is appened to the end of json. here, "hgnc=18315"
		String EXPECTED_OUTPUT = "";	
	
		String idTwoRows = "715"; //gene-id - a duplicate (2 rows)
		
		String databaseFile = "src/test/resources/testData/tabix/index/genes.GeneID.idx.h2.db";
		H2Connection h2 = new H2Connection(databaseFile);
		Connection dbConn = h2.getConn();
		
		// Find index
		FindIndex findIndex = new FindIndex();		
		List<Long> pos2rows = findIndex.find(idTwoRows,  true, dbConn);				
		//System.out.println("Postions:"+Arrays.asList(pos2rows));
		
		HashMap<String,List<String>> key2LinesMap = utils.getZipLinesByIndex(bgzipFile, idTwoRows, pos2rows);		
		//System.out.println("Values from catalog:\n"+Arrays.asList(key2LinesMap.get(idTwoRows)));
		
		utils.writeLines(key2LinesMap, queryResultTxt);
		
		CatalogUtils.assertFileEquals(EXPECTED_RESULTS, QUERY_RESULTS);

		dbConn.close();		
		dbConn = null;
		h2 = null;
	}
	
	
}