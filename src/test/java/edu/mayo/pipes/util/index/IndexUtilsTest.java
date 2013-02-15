package edu.mayo.pipes.util.index;

import static org.junit.Assert.assertEquals;

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
		System.out.println("Testing IndexUtilsTest.testLinexByIndex()..");
		
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
		FindIndex findIndex = new FindIndex(dbConn);		
		List<Long> pos2rows = findIndex.find(idTwoRows);				
		//System.out.println("Postions:"+Arrays.asList(pos2rows));
		
		HashMap<String,List<String>> key2LinesMap = utils.getBgzipLinesByIndex(bgzipFile, idTwoRows, pos2rows);		
		//System.out.println("Values from catalog:\n"+Arrays.asList(key2LinesMap.get(idTwoRows)));
		
		utils.writeLines(key2LinesMap, queryResultTxt);
		
		CatalogUtils.assertFileEquals(EXPECTED_RESULTS, QUERY_RESULTS);

		dbConn.close();		
		dbConn = null;
		h2 = null;
	}


	@Test
	public void testLinesByPostion() throws Exception {
		System.out.println("Testing IndexUtilsTest.testLinexByPosition()..");
		
		File bgzipFile = new File("src/test/resources/testData/tabix/genes.tsv.bgz");
		
		IndexUtils utils = new IndexUtils(bgzipFile);

		File queryResultTxt = new File("src/test/resources/testData/tmpOut/queryResults.lookup.fromDb.txt");
		
		final String EXPECTED_RESULT="19\t58858172\t58864865\t{\"_type\":\"gene\",\"_landmark\":\"19\",\"_strand\":\"-\",\"_minBP\":58858172,\"_maxBP\":58864865,\"gene\":\"A1BG\",\"gene_synonym\":\"A1B; ABG; GAB; HYST2477\",\"note\":\"alpha-1-B glycoprotein; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"1\",\"HGNC\":\"5\",\"HPRD\":\"00726\",\"MIM\":\"138670\"}";
		
		String idTwoRows = "1"; //gene-id - a duplicate (2 rows)
		
		String databaseFile = "src/test/resources/testData/tabix/index/genes.GeneID.idx.h2.db";
		H2Connection h2 = new H2Connection(databaseFile);
		Connection dbConn = h2.getConn();
		
		// Find index
		FindIndex findIndex = new FindIndex(dbConn);		
		List<Long> pos2rows = findIndex.find(idTwoRows);				
		//System.out.println("Postions:"+Arrays.asList(pos2rows));
		
		String line = "";
		for (Long pos : pos2rows) {
			line = utils.getBgzipLineByPosition(pos);
			//System.out.println("Line=\n"+line);
			assertEquals(EXPECTED_RESULT, line);
		}

		dbConn.close();		
		dbConn = null;
		h2 = null;
	}

}
