/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.transform.IdentityPipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.JSON.DrillPipe;
import edu.mayo.pipes.bioinformatics.VCF2VariantPipe;
import edu.mayo.pipes.exceptions.InvalidPipeInputException;
import edu.mayo.pipes.history.HCutPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.util.test.PipeTestUtils;

/**
 *
 * @author m102417
 */
public class OverlapPipeTest {
    
    public OverlapPipeTest() {
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

    public String geneFile = "src/test/resources/testData/tabix/genes.tsv.bgz";
    
    //Tests we need:
    //1. if there are no matches in the query -- put empty thing in there and pass history along
    //2. if there is only one match in the query, make sure history is passed along only once. do not add new line or new row
    //3. if there are more than one match in the query, make sure that we have duplicated rows in the history for each match.

    /**
     * Test of processNextStart method, of class Overlap.
     */
    @Test
    public void testProcessNextStart() throws IOException {
        System.out.println( "Tabix Test!" );
        
        //Example direct tabix query and the results... (note only the JSON will be returned by the overlapPipe)
        //r0240560:tabix m102417$ tabix genes.tsv.bgz 17:41196312-41300000 
        //17	41196312	41277500	{"_type":"gene","_landmark":"17","_strand":"-","_minBP":41196312,"_maxBP":41277500,"gene":"BRCA1","gene_synonym":"BRCAI; BRCC1; BROVCA1; IRIS; PNCA4; PPP1R53; PSCP; RNF53","note":"breast cancer 1, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.","GeneID":"672","HGNC":"1100","HPRD":"00218","MIM":"113705"}
        //17	41231278	41231833	{"_type":"gene","_landmark":"17","_strand":"+","_minBP":41231278,"_maxBP":41231833,"gene":"RPL21P4","gene_synonym":"RPL21_58_1548","note":"ribosomal protein L21 pseudogene 4; Derived by automated computational analysis using gene prediction method: Curated Genomic.","pseudo":"","GeneID":"140660","HGNC":"17959"}
        //17	41277600	41297130	{"_type":"gene","_landmark":"17","_strand":"+","_minBP":41277600,"_maxBP":41297130,"gene":"NBR2","gene_synonym":"NCRNA00192","note":"neighbor of BRCA1 gene 2 (non-protein coding); Derived by automated computational analysis using gene prediction method: BestRefseq.","GeneID":"10230","HGNC":"20691"}
        //17	41286808	41287385	{"_type":"gene","_landmark":"17","_strand":"+","_minBP":41286808,"_maxBP":41287385,"gene":"LOC100505873","note":"Derived by automated computational analysis using gene prediction method: GNOMON. Supporting evidence includes similarity to: 1 EST, 1 Protein","pseudo":"","GeneID":"100505873"}
        //17	41296973	41297272	{"_type":"gene","_landmark":"17","_strand":"+","_minBP":41296973,"_maxBP":41297272,"gene":"HMGN1P29","note":"high mobility group nucleosome binding domain 1 pseudogene 29; Derived by automated computational analysis using gene prediction method: Curated Genomic.","pseudo":"","GeneID":"100885865","HGNC":"39373"} 
        //String query = "my\tfirst\tquery\t{\"_landmark\":\"17\",\"_minBP\":41196312,\"_maxBP\":41277500}";  //1 result
        String query2 = "my\tfirst\tquery\t{\"_landmark\":\"17\",\"_minBP\":41196312,\"_maxBP\":41300000}"; //5 results

        List<History> result = new ArrayList<History>();
        OverlapPipe op = new OverlapPipe(geneFile);
        Pipe<String, History> p2 = new Pipeline<String, History>(new HistoryInPipe() , op);
        p2.setStarts(Arrays.asList(query2));
        while( p2.hasNext()) {            
            result.add(p2.next());
        }
        
        assertEquals(5, result.size()); //5 rows returned
        //System.out.println(result.get(4)); //5 rows returned        
    }

    @Test
    public void testProcessNextStart_OneResult() throws IOException {
        System.out.println( "Tabix Test.. TWO RESULTS!" );
        
        //Example direct tabix query and the results... (note only the JSON will be returned by the overlapPipe)
        //r0240560:tabix m102417$ tabix genes.tsv.bgz 17:41196312-41300000 
        //17	41196312	41277500	{"_type":"gene","_landmark":"17","_strand":"-","_minBP":41196312,"_maxBP":41277500,"gene":"BRCA1","gene_synonym":"BRCAI; BRCC1; BROVCA1; IRIS; PNCA4; PPP1R53; PSCP; RNF53","note":"breast cancer 1, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.","GeneID":"672","HGNC":"1100","HPRD":"00218","MIM":"113705"}
        //{"_type":"gene","_landmark":"17","_strand":"+","_minBP":41231278,"_maxBP":41231833,"gene":"RPL21P4","gene_synonym":"RPL21_58_1548","note":"ribosomal protein L21 pseudogene 4; Derived by automated computational analysis using gene prediction method: Curated Genomic.","pseudo":"","GeneID":"140660","HGNC":"17959"}
        
        //TODO find a query that returns exactly 1 row
        String query = "my\tfirst\tquery\t{\"_landmark\":\"17\",\"_minBP\":41196312,\"_maxBP\":41277500}";  //2 results
        
        List<History> result = new ArrayList<History>();
        OverlapPipe op = new OverlapPipe(geneFile);
        Pipe<String, History> p2 = new Pipeline<String, History>(new HistoryInPipe(), op);
        p2.setStarts(Arrays.asList(query));
        while(p2.hasNext()) {
        	result.add(p2.next());
        }
        //System.out.println("size="+result.size());
        assertEquals(2, result.size()); //two result            
    }

    
    //make sure to test zero results!!!
    @Test 
    public void testProcessNextStart_ZeroResults() throws Exception {
        System.out.println( "Tabix Test.. Zero Results!" );
        
        String query = "my\tfirst\tquery\t{\"_landmark\":\"17\",\"_minBP\":4,\"_maxBP\":41}";  //1 result

        try {
	        List<History> result = new ArrayList<History>();
	        OverlapPipe op = new OverlapPipe(geneFile);
	        Pipe<String, History> p2 = new Pipeline<String, History>(new HistoryInPipe(), op);
	        p2.setStarts(Arrays.asList(query));
	        //assertTrue(p.hasNext()==false);
	        for(int i=0; p2.hasNext(); i++) {
	            p2.next();        	
	        }   
        } catch (Exception e) {
			Assert.fail("INVALID QUERY... EXPECTED EXCEPTION!!!");
		}
    }
    
    /**
     * 1	49482	rs202079915	G	A	.	.	RSPOS=49482;dbSNPBuildID=137;SSR=0;SAO=0;VP=050000000005000002000100;WGT=1;VC=SNV;ASP;OTHERK{"CHROM":"1","POS":"49482","ID":"rs202079915","REF":"G","ALT":"A","QUAL":".","FILTER":".","INFO":{"RSPOS":49482,"dbSNPBuildID":137,"SSR":0,"SAO":0,"VP":"050000000005000002000100","WGT":1,"VC":"SNV","ASP":true,"OTHERKG":true},"_id":"rs202079915","_type":"variant","_landmark":"1","_refAllele":"G","_altAlleles":["A"],"_minBP":49482,"_maxBP":49482}	{}
     */
    @Test 
    public void testProcessNextStart_ZeroResultsEmptyJson() throws Exception {
        System.out.println( "Tabix Test.. Zero Results.. with Empty JSON!" );
        
        String query = "my\tfirst\tquery\t{\"_landmark\":\"1\",\"_minBP\":49482,\"_maxBP\":49482}";  //1 result

        try {
	        List<History> result = new ArrayList<History>();
	        OverlapPipe op = new OverlapPipe(geneFile);
	        Pipe<String, History> p2 = new Pipeline<String, History>(new HistoryInPipe(), op);
	        p2.setStarts(Arrays.asList(query));
	        //assertTrue(p.hasNext()==false);
	        for(int i=0; p2.hasNext(); i++) {
	        	result.add(p2.next());       	
	        }   
	        //System.out.println(result.size());
	        assertEquals("{}", result.get(4)); //EMPTY JSON as last element in the list
        } catch (Exception e) {        	
			//Assert.fail("INVALID QUERY... EXPECTED EXCEPTION!!!");
		}
    }
    
    @Test
    public void testMinBPExtend() throws IOException{
        System.out.println("OverlapPipe: Test if extending minbp works");
        String query = "my\tfirst\tquery\t{\"_landmark\":\"1\",\"_minBP\":49482,\"_maxBP\":49482}";  //1 result
		OverlapPipe op = new OverlapPipe(geneFile, 10000, 0);
		Pipe p2 = new Pipeline(
	                new HistoryInPipe(), 
	                op, 
	                new MergePipe("\t") );
		p2.setStarts(Arrays.asList(query));
		List<String> expected = null;
		List<String> actual = PipeTestUtils.getResults(p2);
		// TODO: compare actual to expected
		//PipeTestUtils.assertListsEqual(expected, actual);
    }
    
    //on chr 17, there is a list of genes around BRCA1 that flow as follows:
    //
    //41,166,622                                                                           41,297,272
    //-----------------------------------------------------------------------------------------------
    //<==VAT1==           <=================BRCA1=======================                   HMGM1P29=>
    //         ==RND2==>               RPL21P4==>                                  LOC100505873=>
    //                                                                                  ====NBR2====>
    String VAT1 ="{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"-\",\"_minBP\":41166622,\"_maxBP\":41174459,\"gene\":\"VAT1\",\"gene_synonym\":\"VATI\",\"note\":\"vesicle amine transport protein 1 homolog (T. californica); Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"10493\",\"HGNC\":\"16919\",\"HPRD\":\"05220\",\"MIM\":\"604631\"}";
    String RND2 ="{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"+\",\"_minBP\":41177258,\"_maxBP\":41184058,\"gene\":\"RND2\",\"gene_synonym\":\"ARHN; RHO7; RhoN\",\"note\":\"Rho family GTPase 2; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"8153\",\"HGNC\":\"18315\",\"HPRD\":\"03332\",\"MIM\":\"601555\"}";
    String BRCA1="{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"-\",\"_minBP\":41196312,\"_maxBP\":41277500,\"gene\":\"BRCA1\",\"gene_synonym\":\"BRCAI; BRCC1; BROVCA1; IRIS; PNCA4; PPP1R53; PSCP; RNF53\",\"note\":\"breast cancer 1, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"672\",\"HGNC\":\"1100\",\"HPRD\":\"00218\",\"MIM\":\"113705\"}";
    String RPL21P4="{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"+\",\"_minBP\":41231278,\"_maxBP\":41231833,\"gene\":\"RPL21P4\",\"gene_synonym\":\"RPL21_58_1548\",\"note\":\"ribosomal protein L21 pseudogene 4; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"140660\",\"HGNC\":\"17959\"}";
    String NBR2="{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"+\",\"_minBP\":41277600,\"_maxBP\":41297130,\"gene\":\"NBR2\",\"gene_synonym\":\"NCRNA00192\",\"note\":\"neighbor of BRCA1 gene 2 (non-protein coding); Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"10230\",\"HGNC\":\"20691\"}";
    String LOC100505873="{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"+\",\"_minBP\":41286808,\"_maxBP\":41287385,\"gene\":\"LOC100505873\",\"note\":\"Derived by automated computational analysis using gene prediction method: GNOMON. Supporting evidence includes similarity to: 1 EST, 1 Protein\",\"pseudo\":\"\",\"GeneID\":\"100505873\"}";
    String HMGM1P29="{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"+\",\"_minBP\":41296973,\"_maxBP\":41297272,\"gene\":\"HMGN1P29\",\"note\":\"high mobility group nucleosome binding domain 1 pseudogene 29; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"100885865\",\"HGNC\":\"39373\"}";

    @Test
    public void testGetCluster() throws IOException{
        System.out.println("Get all the genes around BRCA1");
        String[] paths = {"gene"};
        String query = "x\ty\tz\t{\"_landmark\":\"17\",\"_minBP\":41166622,\"_maxBP\":41297272}";
        OverlapPipe op = new OverlapPipe(geneFile, 0, 0);
        Pipe p = new Pipeline(
                new HistoryInPipe(), 
	        op,
                new DrillPipe(false, paths),
                new PrintPipe()
                );
        p.setStarts(Arrays.asList(query));
        for(int i=1; p.hasNext(); i++){
            History h = (History) p.next();
            if(i==1){
                assertEquals("VAT1",h.get(4));
            }
            if(i==2){
                assertEquals("RND2",h.get(4));
            }
            if(i==3){
                assertEquals("BRCA1",h.get(4));
            }
            if(i==4){
                assertEquals("RPL21P4",h.get(4));
            }
            if(i==5){
                assertEquals("NBR2",h.get(4));
            }
            if(i==6){
                assertEquals("LOC100505873",h.get(4));
            }
            if(i==7){
                assertEquals("HMGN1P29",h.get(4));
            }
        }
    }
    
    @Test
    public void testBRCA1UP() throws IOException{
        System.out.println("Get the genes up-chromosome and including BRCA1");
        String[] paths = {"gene"};
        String query = "x\ty\tz\t" + BRCA1;
        OverlapPipe op = new OverlapPipe(geneFile, 29000, 0);
        Pipe p = new Pipeline(
                new HistoryInPipe(), 
	        op,
                new DrillPipe(false, paths),
                new PrintPipe()
                );
        p.setStarts(Arrays.asList(query));
        for(int i=1; p.hasNext(); i++){
            History h = (History) p.next();
            if(i==1){
                assertEquals("VAT1",h.get(4));
            }
            if(i==2){
                assertEquals("RND2",h.get(4));
            }
            if(i==3){
                assertEquals("BRCA1",h.get(4));
            }
            if(i==4){
                assertEquals("RPL21P4",h.get(4));//gene overlaps BRCA1 so it is included
            }
        }
    }
   
    
    
    @Test
    public void testBRCA1DOWN() throws IOException{
        System.out.println("Get the genes down-chromosome and including BRCA1");
        String[] paths = {"gene"};
        String query = "x\ty\tz\t" + BRCA1;
        OverlapPipe op = new OverlapPipe(geneFile, 0, 19772);
        Pipe p = new Pipeline(
                new HistoryInPipe(), 
	        op,
                new DrillPipe(false, paths),
                new PrintPipe()
                );
        p.setStarts(Arrays.asList(query));
        for(int i=1; p.hasNext(); i++){
            History h = (History) p.next();
            if(i==1){
                assertEquals("BRCA1",h.get(4));
            }
            if(i==2){
                assertEquals("RPL21P4",h.get(4));//gene overlaps BRCA1 so it is included
            }
            if(i==3){
                assertEquals("NBR2",h.get(4));
            }
            if(i==4){
                assertEquals("LOC100505873",h.get(4));
            }
            if(i==5){
                assertEquals("HMGN1P29",h.get(4));
            }
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
    	testColFlag(1, true);
    }
    
    /** Test positive column # as parm with multiple columns in input */
    @Test
    public void testColFlag_cPositive_MultiCols() throws IOException{
    	testColFlag(9, false);
    }
    
    /** Test column -1 as parm with only 1 column in input */
    @Test
    public void testColFlag_cNeg1_1Col() throws IOException{
    	testColFlag(-1, true);
    }
    
    /** Test column -1 as parm with multiple input columns */
    @Test
    public void testColFlag_cNeg1_MultiCols() throws IOException{
    	testColFlag(-1, false);
    }

    /** Test column 0 as parm with multiple input columns - should throw exception */
    @Test (expected=InvalidPipeInputException.class)
    public void testColFlag_c0_MultiCols() throws IOException{
    	testColFlag(0, false);
        fail("Should not make it here - an exception should be thrown before getting this far!");
    }
    
    private void testColFlag(int col, boolean isSingleColumnInput) throws IOException {
        Pipeline p = new Pipeline(
        		new HistoryInPipe(), 
        		new VCF2VariantPipe(),
        		// If we want to work with a single column, then cut the first 8 columns after doing vcf_to_json
        		(isSingleColumnInput ? new HCutPipe( new int[] {1,2,3,4,5,6,7,8} ) : new IdentityPipe()), 
        		new SameVariantPipe("src/test/resources/testData/sameVariantCatalog.tsv.gz", col));
        final String INPUT = "21	26960070	rs116645811	G	A	.	.	.";
        p.setStarts(Arrays.asList(INPUT));
        List<String> actual = PipeTestUtils.getResults(p);
        final String EXPECTED = 
        	(isSingleColumnInput  ?  ""  :  INPUT + "\t")
        	+ "{\"CHROM\":\"21\",\"POS\":\"26960070\",\"ID\":\"rs116645811\",\"REF\":\"G\",\"ALT\":\"A\",\"QUAL\":\".\",\"FILTER\":\".\",\"INFO\":{\".\":true},\"_id\":\"rs116645811\",\"_type\":\"variant\",\"_landmark\":\"21\",\"_refAllele\":\"G\",\"_altAlleles\":[\"A\"],\"_minBP\":26960070,\"_maxBP\":26960070}\t"
       		+ "{\"CHROM\":\"21\",\"POS\":\"26960070\",\"ID\":\"rs116645811\",\"REF\":\"G\",\"ALT\":\"A\",\"QUAL\":\".\",\"FILTER\":\".\",\"INFO\":{\".\":true},\"_id\":\"rs116645811\",\"_type\":\"variant\",\"_landmark\":\"21\",\"_refAllele\":\"G\",\"_altAlleles\":[\"A\"],\"_minBP\":26960070,\"_maxBP\":26960070}";
        PipeTestUtils.assertListsEqual(Arrays.asList(EXPECTED), actual);
    }
}
