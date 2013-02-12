package edu.mayo.pipes.util.index;

import java.sql.Connection;
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
		H2Connection h2 = new H2Connection(databaseFile);
		Connection dbConn = h2.getConn();
		System.out.println(dbConn.isValid(5));
		
		FindIndex findIndex = new FindIndex();
		/*
		HashMap<String,List<Long>> key2posMap = findIndex.find(idToFind, isKeyInteger, dbConn);
		
		System.out.println(key2posMap.size());
		
		for (String name: key2posMap.keySet()){
            String key = name.toString();
            //String value = key2posMap.get(name).toString();  
            System.out.println(key);  
		} */		
	}
	
}
