/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.JSON.SimpleDrillPipe;
import edu.mayo.pipes.JSON.tabix.TabixReader.Iterator;
import edu.mayo.pipes.PrintPipe;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class TabixSearchPipeTest {
    
    public TabixSearchPipeTest() {
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

    public String dataFile = "src/test/resources/testData/tabix/example.gff.gz";
    public String geneFile = "src/test/resources/testData/tabix/genes.tsv.bgz";
    public String tabixIndexFile = "src/test/resources/testData/tabix/example.gff.gz.tbi";
    
    @Test
    public void testProcessNextStart() throws Exception {
        System.out.println("Test Tabix Search Pipe... multiple query results");
        //String query = "{\"_landmark\":\"17\",\"_minBP\":41196312,\"_maxBP\":41300000\"}"; //5 results
        String query = "{\"_landmark\":\"17\",\"_minBP\":41196312,\"_maxBP\":41300000}";
        String[] drills = new String[1];
        drills[0] = "gene";
        Pipe p = new Pipeline(new TabixSearchPipe(geneFile), new SimpleDrillPipe(false, drills));
        p.setStarts(Arrays.asList(query));
        for(int i=0; p.hasNext(); i++){
            List<String> d = (List<String>) p.next();
            if(i==0){
                assertEquals("BRCA1",d.get(0));
            }
            if(i==1){
                assertEquals("RPL21P4",d.get(0));
            }
            if(i==2){
                assertEquals("NBR2",d.get(0));
            }
            if(i==3){
                assertEquals("LOC100505873",d.get(0));
            }
            if(i==4){
                assertEquals("HMGN1P29",d.get(0));
            }
            //make sure we only had 5 elements!
            assertTrue(i != 5);                    
        }
    }

    @Test
    public void testProcessNextStart_EmptyResults() throws Exception {
        String query = "{\"_landmark\":\"17\",\"_minBP\":1,\"_maxBP\":4}";
        String[] drills = new String[1];
        drills[0] = "gene";
        Pipe p = new Pipeline(new TabixSearchPipe(geneFile), new SimpleDrillPipe(false, drills));
        p.setStarts(Arrays.asList(query));
        assertTrue(p.hasNext()==false);
    }    
   
    @Test 
    public void testProcessNextStart_InvalidQuery() {
        String query = "some invalid query";
        String[] drills = new String[1];
        drills[0] = "gene";
        Pipe p;
		try {
			p = new Pipeline(new TabixSearchPipe(geneFile), new SimpleDrillPipe(false, drills));
	        p.setStarts(Arrays.asList(query));
	        for(int i=0; p.hasNext(); i++){
	            p.next(); //throws an exception
	        }
		} catch (Exception e) {
			Assert.fail("INVALID QUERY... EXPECTED EXCEPTION!!!");
		}
    }
    
    @Test (expected=IOException.class) 
    public void testProcessNextStart_InvalidFile() throws Exception {
    	// Test for invalid file - an IOException is expected
        String query = "{\"_landmark\":\"22\",\"_minBP\":41196309,\"_maxBP\":411966310}";
        String[] drills = new String[1];
        drills[0] = "gene";
        Pipe p = new Pipeline(new TabixSearchPipe("invalid-file"), new SimpleDrillPipe(false, drills)); // should throw an exception
        p.setStarts(Arrays.asList(query)); 
    }
    
    @Test
    public void testTQuery() throws Exception {
        String record = "";
        String r1 = "abc123\t.\tgene\t6000\t12000\t.\t+\t.\tID=gene00005";
        String r2 = "abc123\t.\tgene\t8000\t16000\t.\t+\t.\tID=gene00005";
        String query = "abc123:7000-13000";
        TabixSearchPipe op = new TabixSearchPipe(dataFile);
        Iterator records = op.tquery(query);
        for(int i=1;(record = records.next()) != null; i++){
            //System.out.println(record);
            if(i==1){
                assertEquals(r1,record);
            }
            if(i==2){
                assertEquals(r2,record);
            }
        }
    }
    
    @Test
    public void testQuery() throws Exception{
        String record = "";
        TabixSearchPipe op = new TabixSearchPipe(geneFile);
        String brca1 = "{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"-\",\"_minBP\":41196312,\"_maxBP\":41277500,\"gene\":\"BRCA1\",\"gene_synonym\":\"BRCAI; BRCC1; BROVCA1; IRIS; PNCA4; PPP1R53; PSCP; RNF53\",\"note\":\"breast cancer 1, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"672\",\"HGNC\":\"1100\",\"HPRD\":\"00218\",\"MIM\":\"113705\"}";
        Iterator records = op.query(brca1);
        for(int i=1;(record = records.next()) != null; i++){
            System.out.println("Record:"+record);
            String[] s = record.split("\t");
            assertEquals(4, s.length);
            assertEquals("17", s[0]); //chromosome
            assertEquals("41196312", s[1]); //minbp
            assertEquals("41277500", s[2]); //maxbp
            break;
        }
    } 

}
