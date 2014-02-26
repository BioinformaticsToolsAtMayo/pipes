/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.exceptions.InvalidPipeInputException;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.history.HistoryOutPipe;
import edu.mayo.pipes.util.test.PipeTestUtils;

/**
 *
 * @author m102417
 */
public class DrillPipeTest {
    
    public DrillPipeTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of processNextStart method, of class DrillPipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("Drill Pipe Test");
        //note s1 does not have minBP, this will cause a drill to fail in the test, drill failure will result in return of a period '.'
        String s1 = "foo\tbar\t{\"type\":\"gene\",\"chr\":\"17\",\"strand\":\"+\",\"maxBP\":41184058,\"gene\":\"RND2\",\"gene_synonym\":\"ARHN; RHO7; RhoN\",\"note\":\"Rho family GTPase 2; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"8153\",\"HGNC\":\"18315\",\"HPRD\":\"03332\",\"MIM\":\"601555\"}";
        String s2 = "foo\tbar\t{\"type\":\"gene\",\"chr\":\"17\",\"strand\":\"-\",\"minBP\":41196312,\"maxBP\":41277500,\"gene\":\"BRCA1\",\"gene_synonym\":\"BRCAI; BRCC1; BROVCA1; IRIS; PNCA4; PPP1R53; PSCP; RNF53\",\"note\":\"breast cancer 1, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"672\",\"HGNC\":\"1100\",\"HPRD\":\"00218\",\"MIM\":\"113705\"}";
        //String s3 = "foo\tbar\tbaz";
        String s4 = "foo\tbar\t{\"type\":\"gene\",\"chr\":\"17\",\"strand\":\"+\",\"minBP\":41231278,\"maxBP\":41231833,\"gene\":\"RPL21P4\",\"gene_synonym\":\"RPL21_58_1548\",\"note\":\"ribosomal protein L21 pseudogene 4; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"140660\",\"HGNC\":\"17959\"}";

        String[] paths = new String[2];
        paths[0] = "gene";
        paths[1] = "minBP";
        Pipe<String, History> p = new Pipeline(new HistoryInPipe(), new DrillPipe(true, paths));
        p.setStarts(Arrays.asList(s1,s2, s4));
        for(int i=0; p.hasNext(); i++){
        	History history = p.next();
            List<String> drilled = history;
            for(int j=0; j<drilled.size(); j++){
                //System.out.println(drilled.get(j));
                if(i==1 && j==2){//j==0 is foo, j==1 is bar, j==2 starts the drilled data.
                    assertEquals("BRCA1", drilled.get(j));
                }
                if(i==1 && j==4){ //should have the raw json in column 4
                    String[] split = s2.split("\t");
                    assertEquals(split[2], drilled.get(j));
                }
                //check to make sure a failed drill outputs another column with content '.'
                if(i==0 && j==3){
                    assertEquals(".", drilled.get(j));
                }
                
                //the last column did not have json, so there is nothing to drill... ensure that the drill outputs a '.'
                if(i==3 && j==0){
                    assertEquals("foo", drilled.get(j));
                }
                if(i==3 && j==1){
                    assertEquals("bar", drilled.get(j));
                }
                if(i==3 && j==2){
                    assertEquals(".", drilled.get(j));
                }
                if(i==3 && j==3){
                    assertEquals(".", drilled.get(j));
                }
//                if(i==2 && j==4){
//                    assertEquals("baz", drilled.get(j));
//                }
            }
        }

        
    }
    
    @Test
    public void testSingleColumnDrill() {
        System.out.println("Test single column drill");
        //note s1 does not have minBP, this will cause a drill to fail in the test, drill failure will result in return of a period '.'
        String s1 = "{\"type\":\"gene\",\"chr\":\"17\",\"strand\":\"+\",\"maxBP\":41184058,\"gene\":\"RND2\",\"gene_synonym\":\"ARHN; RHO7; RhoN\",\"note\":\"Rho family GTPase 2; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"8153\",\"HGNC\":\"18315\",\"HPRD\":\"03332\",\"MIM\":\"601555\"}";
        String s2 = "{\"type\":\"gene\",\"chr\":\"17\",\"strand\":\"-\",\"minBP\":41196312,\"maxBP\":41277500,\"gene\":\"BRCA1\",\"gene_synonym\":\"BRCAI; BRCC1; BROVCA1; IRIS; PNCA4; PPP1R53; PSCP; RNF53\",\"note\":\"breast cancer 1, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"672\",\"HGNC\":\"1100\",\"HPRD\":\"00218\",\"MIM\":\"113705\"}";
        String s3 = "{\"type\":\"gene\",\"chr\":\"17\",\"strand\":\"+\",\"minBP\":41231278,\"maxBP\":41231833,\"gene\":\"RPL21P4\",\"gene_synonym\":\"RPL21_58_1548\",\"note\":\"ribosomal protein L21 pseudogene 4; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"140660\",\"HGNC\":\"17959\"}";

        String[] paths = new String[2];
        paths[0] = "gene";
        paths[1] = "minBP";
        //note the constructor uses -5... this is wrong, so we want to test that the pipe correctly re-adjusts the value
        Pipe<String, String> p = new Pipeline(new HistoryInPipe(), new DrillPipe(false, paths,-5), new MergePipe(" "));
        p.setStarts(Arrays.asList(s1,s2, s3));
        for(int i=0; p.hasNext(); i++){
            String s = (String) p.next();
            if(i==0){
                assertEquals("RND2 .", s);
            }
            if(i==1){
                assertEquals("BRCA1 41196312", s);
            }
            if(i==2){
                assertEquals("RPL21P4 41231278", s);
            }
        }
    }
   
    
    @Test
    /** Test multiple JSON paths drilled, positive column # for the JSON */
    public void testMultiPath_PosCol() {
        System.out.println("DrillPipeTest.testMultiPath_PosCol(): Test multiple JSON paths drilled, specifying a positive column number");
        //note s1 does not have minBP, this will cause a drill to fail in the test, drill failure will result in return of a period '.'
        String s1 = "17\t41231278\t41184058\t{\"type\":\"gene\",\"chr\":\"17\",\"strand\":\"+\",\"minBP\":41231278,\"maxBP\":41184058,\"gene\":\"RND2\"}\tSomeOtherTextOnTheEnd";
        String[] drillPaths = { "gene", "chr", "minBP", "maxBP" };
        Pipe<String, String> p = new Pipeline(new HistoryInPipe(), new DrillPipe(false, drillPaths, 4), new MergePipe("\t"));
        p.setStarts(Arrays.asList(s1));
        List<String> actual = PipeTestUtils.getResults(p);
        List<String> expected = Arrays.asList(
                "17\t41231278\t41184058\tSomeOtherTextOnTheEnd\tRND2\t17\t41231278\t41184058"
        		);
        PipeTestUtils.assertListsEqual(expected, actual);
    }

    @Test
    /** Test multiple JSON paths drilled, negative column # for the JSON */
    public void testMultiPath_NegCol() {
        System.out.println("DrillPipeTest.testMultiPath_NegCol(): Test multiple JSON paths drilled, specifying a negative column number");
        //note s1 does not have minBP, this will cause a drill to fail in the test, drill failure will result in return of a period '.'
        String s1 = "17\t41231278\t41184058\t{\"type\":\"gene\",\"chr\":\"17\",\"strand\":\"+\",\"minBP\":41231278,\"maxBP\":41184058,\"gene\":\"RND2\"}\tSomeOtherTextOnTheEnd";
        String[] drillPaths = { "gene", "chr", "minBP", "maxBP" };
        Pipe<String, String> p = new Pipeline(new HistoryInPipe(), new DrillPipe(false, drillPaths, -2), new MergePipe("\t"));
        p.setStarts(Arrays.asList(s1));
        List<String> actual = PipeTestUtils.getResults(p);
        List<String> expected = Arrays.asList(
                "17\t41231278\t41184058\tSomeOtherTextOnTheEnd\tRND2\t17\t41231278\t41184058"
        		);
        PipeTestUtils.assertListsEqual(expected, actual);
    }

    
  //  @Test
    public void testRemoveDrillColumnMetadata() {
    	System.out.println("Test RemoveDrillColumnMetadata..");
    	List<String> input = Arrays.asList(
    		"##Header start",
    		"#Chrom\toperation",
    		"1\t{\"Key\":\"Value\"}"
    	);
    	
    	String[] drillCols = { "Key" };
    	
    	Pipeline pipe = new Pipeline(
    		new HistoryInPipe(),
    		new DrillPipe(false, drillCols),
    		new HistoryOutPipe(),
    		new PrintPipe()
    		);
    	pipe.setStarts(input);
    	List<String> actual = PipeTestUtils.getResults(pipe);
    	System.out.println("actual="+Arrays.asList(actual));
    	List<String> expected = Arrays.asList(
    		"##Header start",
    		"#Chrom\toperation.Key",
    		"1\tValue"
    	);
    	
    	PipeTestUtils.assertListsEqual(expected, actual);
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
    	testColFlag(1, true);
    }
    
    /** Test positive column # as parm with multiple columns in input */
    @Test
    public void testColFlag_cPositive_multiCols() throws IOException{
    	testColFlag(9, false);
    }
    
    /** Test column -1 as parm with only 1 column in input */
    @Test
    public void testColFlag_cNeg1_1Col() throws IOException{
    	testColFlag(-1, true);
    }
    
    /** Test column -1 as parm with multiple input columns */
    @Test
    public void testColFlag_cNeg1_multiCols() throws IOException{
    	testColFlag(-1, false);
    }

    /** Test column 0 as parm with multiple input columns - should throw exception */
    @Test (expected=InvalidPipeInputException.class)
    public void testColFlag_c0_MultiCols() throws IOException{
    	testColFlag(0, false);
        fail("Should not make it here - an exception should be thrown before getting this far!");
    }
    
    private void testColFlag(int col, boolean isSingleColumnInput) {
    	final String INPUT = isSingleColumnInput 
    			? "{\"ID\":31,\"Key\":\"volume\",\"Val\":10}"
    			: "1\t2\t3\t4\t5\t6\t7\t8\t{\"ID\":31,\"Key\":\"volume\",\"Val\":10}";
        Pipeline p = new Pipeline(new HistoryInPipe(), new DrillPipe(false, new String[] {"ID"}, col));
        p.setStarts(Arrays.asList(INPUT));
        List<String> actual = PipeTestUtils.getResults(p);
        final String EXPECTED = (isSingleColumnInput  ?  ""  :  "1\t2\t3\t4\t5\t6\t7\t8\t") + "31";
        PipeTestUtils.assertListsEqual(Arrays.asList(EXPECTED), actual);
    }
    

}
