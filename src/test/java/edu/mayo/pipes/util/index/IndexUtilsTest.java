package edu.mayo.pipes.util.index;

import java.io.File;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import edu.mayo.pipes.JSON.lookup.lookupUtils.IndexUtils;
import edu.mayo.pipes.util.CatalogUtils;

public class IndexUtilsTest {
	
	@Test
	public void testLinesByIndex() throws Exception {
		System.out.println("Testing LookupPipeTest.testLinexByIndex()..");
		
		IndexUtils utils = new IndexUtils();
		File bgzipFile = new File("src/test/resources/testData/tabix/genes.tsv.bgz");
		File queryResultTxt = new File("src/test/resources/testData/tmpOut/queryResults.lookup.fromDb.txt");
		
		final String EXPECTED_RESULTS="src/test/resources/testData/tabix/expected.results.lookup.txt";
		final String QUERY_RESULTS = "src/test/resources/testData/tmpOut/queryResults.lookup.fromDb.txt";
		
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
