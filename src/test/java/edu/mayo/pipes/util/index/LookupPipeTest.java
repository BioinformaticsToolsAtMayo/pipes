package edu.mayo.pipes.util.index;

import java.io.File;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

public class LookupPipeTest {

	@Test
	public void test() throws Exception {
		// the last entry, after json string is the lookup-id. here i used hgnc-id
		String INPUT = "chr\t41177258\t41184058\t18315\t{\"type\":\"gene\",\"chr\":\"17\",\"strand\":\"+\",\"minBP\":41177258,\"maxBP\":41184058,\"gene\":\"RND2\",\"gene_synonym\":\"ARHN; RHO7; RhoN\",\"note\":\"Rho family GTPase 2\",\"GeneID\":\"8153\",\"HPRD\":\"03332\",\"MIM\":\"601555\"}";
	
		// if the lookup if found, the related data is appened to the end of json. here, "hgnc=18315"
		String EXPECTED_OUTPUT = "";	
	
		String idToFind = "18315";
		
		boolean isKeyInteger = true;
		
		String databaseFile = "/src/test/resources/testData/tabix/index/genes.HGNC.idx.h2.db";
		
		File bgzipFile = new File("resources/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz");
		File tmpTxt = new File("resources/tmp.txt");
	
		H2Connection h2 = new H2Connection(databaseFile);
		Connection dbConn = h2.getConn();
		//System.out.println(dbConn.isValid(5));
		
		// 1. Create table
		//h2.createTable(false, 200, dbConn);
		
		// 2. 
	    //utils.zipIndexesToTextFile(bgzipFile, "\t", 3, null, tmpTxt);

		// 3. 
		// textIndexesToDb(dbConn, false, tmpTxt);

		// 4. 
		//createDbTableIndex(dbConn);

		// 5. find index
		/*
		FindIndex findIndex = new FindIndex();		
		HashMap<String,List<Long>> key2posMap = findIndex.find(idToFind, isKeyInteger, dbConn);		
		System.out.println(key2posMap.size());
		
		for (String name: key2posMap.keySet()){
            String key = name.toString();
            //String value = key2posMap.get(name).toString();  
            System.out.println(key);
            System.out.println(Arrays.asList(key2posMap.get(name)));
		} */
				
		// 6. 
		// For each row-number retrieved from the above findIndex.find, get the row from the tabix-catalog-file
		
		// 7.
		// HashMap<String,List<String>> key2LinesMap = utils.getZipLinesByIndex(bgzipFile, key2posMap);
		
		dbConn.close();		
		h2.closeConn();
	}
	
}
