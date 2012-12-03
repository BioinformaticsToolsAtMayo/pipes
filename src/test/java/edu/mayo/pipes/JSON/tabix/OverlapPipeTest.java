/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.JSON.tabix.TabixReader.Iterator;
import edu.mayo.pipes.UNIX.CatGZPipe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.*;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import static org.junit.Assert.*;

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

    
    //Tests we need:
    //1. if there are no matches in the query -- put empty thing in there and pass history along
    //2. if there is only one match in the query, make sure history is passed along only once. do not add new line or new row
    //3. if there are more than one match in the query, make sure that we have duplicated rows in the history for each match.

//
//    
//    
//    /**
//     * Test of processNextStart method, of class Overlap.
//     */
//    @Test
//    public void testProcessNextStart() throws IOException {
//        System.out.println( "Tabix Test!" );
//        
//        //        Pipe p = new Pipeline( new Overlap(dataFile) );
//        //        p.setStarts(Arrays.asList(""));
//        //        for(int i=0; p.hasNext(); i++){
//        //            p.next();
//        //        }
//        
//        //Overlap instance = null;
//        //List expResult = null;
//        //List result = instance.processNextStart();
//        //assertEquals(expResult, result);
//        
//        
//        //String dataFile = "/Users/m102417/tabixtest/example.gff.gz";
//        //String tabixIndexFile = "/Users/m102417/tabixtest/example.gff.gz.tbi";
//        
//        //Example direct tabix query and the results... (note only the JSON will be returned by the overlapPipe)
//        //r0240560:tabix m102417$ tabix genes.tsv.bgz 17:41196312-41300000 
//        //17	41196312	41277500	{"_type":"gene","_landmark":"17","_strand":"-","_minBP":41196312,"_maxBP":41277500,"gene":"BRCA1","gene_synonym":"BRCAI; BRCC1; BROVCA1; IRIS; PNCA4; PPP1R53; PSCP; RNF53","note":"breast cancer 1, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.","GeneID":"672","HGNC":"1100","HPRD":"00218","MIM":"113705"}
//        //17	41231278	41231833	{"_type":"gene","_landmark":"17","_strand":"+","_minBP":41231278,"_maxBP":41231833,"gene":"RPL21P4","gene_synonym":"RPL21_58_1548","note":"ribosomal protein L21 pseudogene 4; Derived by automated computational analysis using gene prediction method: Curated Genomic.","pseudo":"","GeneID":"140660","HGNC":"17959"}
//        //17	41277600	41297130	{"_type":"gene","_landmark":"17","_strand":"+","_minBP":41277600,"_maxBP":41297130,"gene":"NBR2","gene_synonym":"NCRNA00192","note":"neighbor of BRCA1 gene 2 (non-protein coding); Derived by automated computational analysis using gene prediction method: BestRefseq.","GeneID":"10230","HGNC":"20691"}
//        //17	41286808	41287385	{"_type":"gene","_landmark":"17","_strand":"+","_minBP":41286808,"_maxBP":41287385,"gene":"LOC100505873","note":"Derived by automated computational analysis using gene prediction method: GNOMON. Supporting evidence includes similarity to: 1 EST, 1 Protein","pseudo":"","GeneID":"100505873"}
//        //17	41296973	41297272	{"_type":"gene","_landmark":"17","_strand":"+","_minBP":41296973,"_maxBP":41297272,"gene":"HMGN1P29","note":"high mobility group nucleosome binding domain 1 pseudogene 29; Derived by automated computational analysis using gene prediction method: Curated Genomic.","pseudo":"","GeneID":"100885865","HGNC":"39373"} 
//        String query = "my\tfirst\tquery\t{\"_landmark\":\"17\",\"_minBP\":41196312,\"_maxBP\":41277500\"}";  //1 result
//        String query2 = "my\tfirst\tquery\t{\"_landmark\":\"17\",\"_minBP\":41196312,\"_maxBP\":41300000\"}"; //5 results
//              
//        
//        List<String> result = new ArrayList<String>();
//        OverlapPipe op = new OverlapPipe(geneFile);
//        Pipe p2 = new Pipeline(new SplitPipe("\t"), op, new PrintPipe());
//        p2.setStarts(Arrays.asList(query, query2));
//        for(int i=0; p2.hasNext(); i++) {
//            p2.next();
//        	//result.addAll((List<String>)p2.next());
//        }
//        
//        System.out.println(result.size());    
//    }
    
    //make sure to test zero results!!!
}
