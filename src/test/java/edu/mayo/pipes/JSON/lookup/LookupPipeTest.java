package edu.mayo.pipes.JSON.lookup;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;

public class LookupPipeTest {

	@Test
	public void testLookupPipe() throws Exception {
		String dataFile = "src/test/resources/testData/tabix/genes.tsv.bgz";
	    String indexFile = "src/test/resources/testData/tabix/index/genes.HGNC.idx.h2.db";
	    
	    LookupPipe lookup = new LookupPipe(dataFile, indexFile, 3);
	    
	    final String EXPECTED_RESULT = "{\"_type\":\"gene\",\"_landmark\":\"12\",\"_strand\":\"-\",\"_minBP\":9381129,\"_maxBP\":9386803,\"gene\":\"A2MP1\",\"gene_synonym\":\"A2MP\",\"note\":\"alpha-2-macroglobulin pseudogene 1; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"pseudo\":\"\",\"GeneID\":\"3\",\"HGNC\":\"8\"}";
	    
	    Pipe<String, History> p = new Pipeline(new HistoryInPipe(), lookup);
	    p.setStarts(Arrays.asList("ABC\tDEF\t8"));
	    //p.setStarts(Arrays.asList("GHI\tJKL\t7"));
	    
	    while(p.hasNext()) {	    	
	    	History history = (History) p.next();
	    	String result = history.get(3);
		    assertEquals(EXPECTED_RESULT, result);
	    }	
	    
	}
	
	@Test
	public void testLookupPipe_Empty() throws Exception {
		String dataFile = "src/test/resources/testData/tabix/genes.tsv.bgz";
	    String indexFile = "src/test/resources/testData/tabix/index/genes.HGNC.idx.h2.db";
	    
	    LookupPipe lookup = new LookupPipe(dataFile, indexFile, 1);
	    
	    // Look for HGNC Id that is "."
	    final String EXPECTED_RESULT = "{}";
	    
	    Pipe<String, History> p = new Pipeline(new HistoryInPipe(), lookup);
	    p.setStarts(Arrays.asList("."));
	    
	    while(p.hasNext()) {
	    	History history = (History) p.next();            
	    	String result = history.get(1);
		    assertEquals(EXPECTED_RESULT, result);
	    }	
	    
	}

	@Test
	public void testLookupPipe_KeyColumnIsIntegerButStringGiven() throws Exception {
		String dataFile = "src/test/resources/testData/tabix/genes.tsv.bgz";
	    String indexFile = "src/test/resources/testData/tabix/index/genes.HGNC.idx.h2.db";
	    
	    LookupPipe lookup = new LookupPipe(dataFile, indexFile, 4);
	    
	    final String EXPECTED_RESULT = "{}";
	    
	    Pipe<String, History> p = new Pipeline(new HistoryInPipe(), lookup);
	    p.setStarts(Arrays.asList("Y\t28740815\t28780802\tJUNK"));
	    
	    while(p.hasNext()) {	    	
	    	History history = (History) p.next();            
	    	String result = history.get(history.size()-1);
		    assertEquals(EXPECTED_RESULT, result);
	    }	
	    
	}

}
