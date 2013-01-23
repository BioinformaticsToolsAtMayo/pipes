/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import static org.junit.Assert.assertEquals;

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
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;

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
        ArrayList<String> results = new ArrayList<String>();
        String query = "my\tfirst\tquery\t{\"_landmark\":\"1\",\"_minBP\":49482,\"_maxBP\":49482}";  //1 result
	OverlapPipe op = new OverlapPipe(geneFile, 10000, 0);
	Pipe p2 = new Pipeline(
                new HistoryInPipe(), 
                op, 
                new MergePipe("\t"),
                new PrintPipe());
	p2.setStarts(Arrays.asList(query));
        for(int i=0; p2.hasNext(); i++) {
            results.add((String)p2.next());       	
	} 
    }
        
}
