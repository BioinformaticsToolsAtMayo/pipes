package edu.mayo.pipes.JSON.lookup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.bioinformatics.VCF2VariantPipe;
import edu.mayo.pipes.exceptions.InvalidPipeInputException;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.util.test.PipeTestUtils;

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
	
	
    // ===================================================================
    // Test pipe where we choose the column in the history where the variant JSON comes from:
    // col 1 with only 1 column
    // col positive with multiple columns
    // col -1 with only 1 column
    // col -1 with multiple columns
    // col 0 - should throw error
    // ===================================================================
    
    /** Test column 1 as parm with only 1 column in input */
    @Test
    public void testColFlag_c1_1Col() throws IOException{
        Pipeline p = new Pipeline(new HistoryInPipe(), new LookupPipe(getGenesCatalogFile(), getGeneIdIndexFile(), 1));
        p.setStarts(getOneMatchInput1Col());
        List<String> actual = PipeTestUtils.getResults(p);
        PipeTestUtils.assertListsEqual(getOneMatchExpected2Col(), actual);
    }

    
    
    /** Test positive column # as parm with multiple columns in input */
    @Test
    public void testColFlag_cPositive_multiCols() throws IOException{
        Pipeline p = new Pipeline(new HistoryInPipe(), new LookupPipe(getGenesCatalogFile(), getGeneIdIndexFile(), 9));
        p.setStarts(getOneMatchInput9Cols());
        List<String> actual = PipeTestUtils.getResults(p);
        PipeTestUtils.assertListsEqual(getOneMatchExpected10Col(), actual);
    }

    
    /** Test column -1 as parm with only 1 column in input */
    @Test
    public void testColFlag_cNeg1_1Col() throws IOException{
    	Pipeline p = new Pipeline(new HistoryInPipe(), new LookupPipe(getGenesCatalogFile(), getGeneIdIndexFile(), -1));
        p.setStarts(getOneMatchInput1Col());
        List<String> actual = PipeTestUtils.getResults(p);
        PipeTestUtils.assertListsEqual(getOneMatchExpected2Col(), actual);
    }

    
    /** Test column -1 as parm with multiple input columns */
    @Test
    public void testColFlag_cNeg1_multiCols() throws IOException{
        Pipeline p = new Pipeline(new HistoryInPipe(), new LookupPipe(getGenesCatalogFile(), getGeneIdIndexFile(), -1));
        p.setStarts(getOneMatchInput9Cols());
        List<String> actual = PipeTestUtils.getResults(p);
        PipeTestUtils.assertListsEqual(getOneMatchExpected10Col(), actual);
    }

    /** Test column 0 as parm with multiple input columns - should throw exception */
    @Test (expected=InvalidPipeInputException.class)
    public void testColFlag_c0_MultiCols() throws IOException{
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), new LookupPipe(getGenesCatalogFile(), getGeneIdIndexFile(), 0));
        p.setStarts(getOneMatchInput9Cols());
        List<String> actual = PipeTestUtils.getResults(p);
        fail("Should not make it here - an exception should be thrown before getting this far!");
    }
    
    
    private String getGenesCatalogFile() {
    	return "src/test/resources/testData/tabix/genes.tsv.bgz";
    }
    private String getGeneIdIndexFile() {
    	return "src/test/resources/testData/tabix/index/genes.gene.idx.h2.db";
    }
    private List<String> getOneMatchInput1Col() {
    	return Arrays.asList("BRCA1");
    }

    private List<String> getOneMatchInput9Cols() {
    	return Arrays.asList(".	.	.	.	.	.	.	.	BRCA1");
    }

    private List<String> getOneMatchExpected10Col() {
        List<String> expected = Arrays.asList(
        	".	.	.	.	.	.	.	.	BRCA1	{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"-\",\"_minBP\":41196312,\"_maxBP\":41277500,\"gene\":\"BRCA1\",\"gene_synonym\":\"BRCAI; BRCC1; BROVCA1; IRIS; PNCA4; PPP1R53; PSCP; RNF53\",\"note\":\"breast cancer 1, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"672\",\"HGNC\":\"1100\",\"HPRD\":\"00218\",\"MIM\":\"113705\"}"
        );
        return expected;
    }
    private List<String> getOneMatchExpected2Col() {
        List<String> expected = Arrays.asList(
        	"BRCA1	{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"-\",\"_minBP\":41196312,\"_maxBP\":41277500,\"gene\":\"BRCA1\",\"gene_synonym\":\"BRCAI; BRCC1; BROVCA1; IRIS; PNCA4; PPP1R53; PSCP; RNF53\",\"note\":\"breast cancer 1, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"672\",\"HGNC\":\"1100\",\"HPRD\":\"00218\",\"MIM\":\"113705\"}"
        );
        return expected;
    }

}
