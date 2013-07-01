package edu.mayo.pipes.JSON.lookup.lookupUtils;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.mayo.pipes.util.index.FindIndex;
import edu.mayo.pipes.util.index.H2Connection;
import edu.mayo.pipes.util.test.FileCompareUtils;

public class IndexUtilsTest {
	
    @Rule
    public TemporaryFolder tFolder = new TemporaryFolder();

	public File tempFolder;
	public File QUERY_RESULTS;
	
	@Before
	public void createTestData() throws IOException {
		tempFolder = tFolder.newFolder("tempDir");
		QUERY_RESULTS = new File(tempFolder, "queryResults.lookup.fromDb.txt");
	    	
	    //System.out.println(tempFolder.getPath() +"::"+ output.getPath());
	}
	
	@Test
	public void testLinesByIndex() throws Exception {
		System.out.println("Testing IndexUtilsTest.testLinexByIndex()..");
		
		IndexUtils utils = new IndexUtils();
		File bgzipFile = new File("src/test/resources/testData/tabix/genes.tsv.bgz");
		//File queryResultTxt = new File("src/test/resources/testData/tmpOut/queryResults.lookup.fromDb.txt");
		
		final String EXPECTED_RESULTS="src/test/resources/testData/tabix/expected.results.lookup.txt";
		//final String QUERY_RESULTS = "src/test/resources/testData/tmpOut/queryResults.lookup.fromDb.txt";
		
		String idTwoRows = "715"; //gene-id - a duplicate (2 rows)
		
		String databaseFile = "src/test/resources/testData/tabix/index/genes.GeneID.idx.h2.db";
		H2Connection h2 = new H2Connection(databaseFile);
		Connection dbConn = h2.getConn();
		
		// Find index
		FindIndex findIndex = new FindIndex(dbConn, false);		
		List<Long> pos2rows = findIndex.find(idTwoRows);
		//System.out.println("Postions:"+Arrays.asList(pos2rows));
		
		HashMap<String,List<String>> key2LinesMap = utils.getBgzipLinesByIndex(bgzipFile, idTwoRows, pos2rows);		
		//System.out.println("Values from catalog:\n"+Arrays.asList(key2LinesMap.get(idTwoRows)));
		
		utils.writeLines(key2LinesMap, QUERY_RESULTS);
		
		FileCompareUtils.assertFileEquals(EXPECTED_RESULTS, QUERY_RESULTS.getPath());

		dbConn.close();		
		dbConn = null;
		h2 = null;
	}


	@Test
	public void testLinesByPosition() throws Exception {
		System.out.println("Testing IndexUtilsTest.testLinexByPosition()..");
		
		String bgzipFile = "src/test/resources/testData/tabix/genes.tsv.bgz";
		String indexFile = "src/test/resources/testData/tabix/index/genes.GeneID.idx.h2.db";
		
		String[] expected = {
				"19\t58858172\t58864865\t{\"_type\":\"gene\",\"_landmark\":\"19\",\"_strand\":\"-\",\"_minBP\":58858172,\"_maxBP\":58864865,\"gene\":\"A1BG\",\"gene_synonym\":\"A1B; ABG; GAB; HYST2477\",\"note\":\"alpha-1-B glycoprotein; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"1\",\"HGNC\":\"5\",\"HPRD\":\"00726\",\"MIM\":\"138670\"}"
		};
		String[] actual = getMatchingRows(bgzipFile, indexFile, "1", false);  // GeneId="1" will get a duplicate (2 rows) 
		assertArrayEquals(expected, actual);
	}

	@Test
	public void testCaseSensitiveLookup() throws Exception {
		System.out.println("Testing IndexUtilsTest.testCaseSensitiveLookup()..");
		
		String bgzipFile 	= "src/test/resources/testData/tabix/genes.tsv.bgz";
		String databaseFile = "src/test/resources/testData/tabix/index/genes.gene.idx.h2.db";

		// Find key - first by default (non-case sensitive) - actual gene name is "C1orf170"
		String[] expected = { 
				"1	910579	917473	{\"_type\":\"gene\",\"_landmark\":\"1\",\"_strand\":\"-\",\"_minBP\":910579,\"_maxBP\":917473,\"gene\":\"C1orf170\",\"gene_synonym\":\"RP11-54O7.8\",\"note\":\"chromosome 1 open reading frame 170; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"84808\",\"HGNC\":\"28208\"}"
		};
		// NOTE: Actual cap is: C1orf170
		// This should match since it's not case-sensitive
		String[] actual = getMatchingRows(bgzipFile, databaseFile, "C1ORF170", false);
		assertArrayEquals(expected, actual);
		
		// Find key - NOT case sensitive (should be one match)
		actual = getMatchingRows(bgzipFile, databaseFile, "c1orf170", false);
		assertArrayEquals(expected, actual);

		// Find key - NOT case sensitive (should be one match)
		actual = getMatchingRows(bgzipFile, databaseFile, "C1orf170", false);
		assertArrayEquals(expected, actual);

		//----------- Now case-sensitive flag --------------
		
		// Find key - case sensitive (none should be found)
		actual = getMatchingRows(bgzipFile, databaseFile, "C1ORF170", true);
		assertArrayEquals(new String[] { }, actual);

		// Find key - case sensitive (none should be found)
		actual = getMatchingRows(bgzipFile, databaseFile, "c1Orf170", true);
		assertArrayEquals(new String[] { }, actual);

		// Find key - case sensitive with actual gene name with correct case
		actual = getMatchingRows(bgzipFile, databaseFile, "C1orf170", true);
		assertArrayEquals(expected, actual);
	}
	
	private String[] getMatchingRows(String bgzipFile, String indexFile, String keyToFind, boolean isCaseSensitive) throws SQLException, IOException {
		H2Connection h2 = new H2Connection(indexFile);
		Connection dbConn = h2.getConn();
		
		FindIndex findIndex = new FindIndex(dbConn, isCaseSensitive);		
		List<Long> positions = findIndex.find(keyToFind);				

		IndexUtils utils = new IndexUtils(new File(bgzipFile));
		ArrayList<String> lines = new ArrayList<String>();
		for (Long pos : positions) {
			lines.add(utils.getBgzipLineByPosition(pos));
		}

		dbConn.close();		
		dbConn = null;
		h2 = null;
		
		return lines.toArray(new String[lines.size()]);
	}
}
