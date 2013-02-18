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
	    
	    LookupPipe lookup = new LookupPipe(indexFile, dataFile);
	    
	    String hgncid = "8";
	    
	    String EXPECTED_RESULT = "{\"_type\":\"gene\",\"_landmark\":\"12\",\"_strand\":\"-\",\"_minBP\":9381129,\"_maxBP\":9386803,\"gene\":\"A2MP1\",\"gene_synonym\":\"A2MP\",\"note\":\"alpha-2-macroglobulin pseudogene 1; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"pseudo\":\"\",\"GeneID\":\"3\",\"HGNC\":\"8\"}";
	    String RESULT="";
	    
	    Pipe<String, History> p = new Pipeline(new HistoryInPipe(), lookup);
	    p.setStarts(Arrays.asList(hgncid));
	    
	    for(int i=0; p.hasNext(); i++) {	    	
	    	History history = (History) p.next();            
	    	RESULT= history.get(1);
	    }	
	    
	    assertEquals(EXPECTED_RESULT, RESULT);
	}
	
	@Test
	public void testLookupPipe_Empty() throws Exception {
		String dataFile = "src/test/resources/testData/tabix/genes.tsv.bgz";
	    String indexFile = "src/test/resources/testData/tabix/index/genes.HGNC.idx.h2.db";
	    
	    LookupPipe lookup = new LookupPipe(indexFile, dataFile);
	    
	    String hgncid = ".";
	    
	    String EXPECTED_RESULT = "{}";
	    String RESULT="";
	    
	    Pipe<String, History> p = new Pipeline(new HistoryInPipe(), lookup);
	    p.setStarts(Arrays.asList(hgncid));
	    
	    for(int i=0; p.hasNext(); i++) {	    	
	    	History history = (History) p.next();            
	    	RESULT= history.get(1);
	    }	
	    
	    assertEquals(EXPECTED_RESULT, RESULT);
	}

}
