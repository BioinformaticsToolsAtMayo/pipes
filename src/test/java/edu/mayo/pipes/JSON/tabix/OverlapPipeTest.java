/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import edu.mayo.pipes.JSON.tabix.TabixReader.Iterator;
import java.io.IOException;
import java.util.List;
import org.junit.*;
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

    public String dataFile = "src/test/resources/testData/tabix/example.gff.gz";
    public String geneFile = "src/test/resources/testData/tabix/genes.tsv.bgz";
    public String tabixIndexFile = "src/test/resources/testData/tabix/example.gff.gz.tbi";
    
    @Test
    public void testTQuery() throws IOException{
        System.out.println("Test TQuery");
        String record = "";
        String r1 = "abc123\t.\tgene\t6000\t12000\t.\t+\t.\tID=gene00005";
        String r2 = "abc123\t.\tgene\t8000\t16000\t.\t+\t.\tID=gene00005";
        String query = "abc123:7000-13000";
        OverlapPipe op = new OverlapPipe(dataFile);
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
    public void testQuery() throws IOException{
        System.out.println("Test Query Based on JSON input");
        String record = "";
        OverlapPipe op = new OverlapPipe(geneFile);
        String brca1 = "{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"-\",\"_minBP\":41196312,\"_maxBP\":41277500,\"gene\":\"BRCA1\",\"gene_synonym\":\"BRCAI; BRCC1; BROVCA1; IRIS; PNCA4; PPP1R53; PSCP; RNF53\",\"note\":\"breast cancer 1, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"672\",\"HGNC\":\"1100\",\"HPRD\":\"00218\",\"MIM\":\"113705\"}";
        Iterator records = op.query(brca1);
        for(int i=1;(record = records.next()) != null; i++){
            System.out.println(record);
            String[] s = record.split("\t");
            assertEquals(4, s.length);
            assertEquals("17", s[0]);
            assertEquals("41196312", s[1]); 
            assertEquals("41277500", s[2]);
        }
    }
    
    

    /**
     * Test of processNextStart method, of class Overlap.
     */
    @Test
    public void testProcessNextStart() throws IOException {
        System.out.println( "Tabix Test!" );
        

        
//        Pipe p = new Pipeline( new Overlap(dataFile) );
//        p.setStarts(Arrays.asList(""));
//        for(int i=0; p.hasNext(); i++){
//            p.next();
//        }
        
        //Overlap instance = null;
        //List expResult = null;
        //List result = instance.processNextStart();
        //assertEquals(expResult, result);
        
        
        //String dataFile = "/Users/m102417/tabixtest/example.gff.gz";
        //String tabixIndexFile = "/Users/m102417/tabixtest/example.gff.gz.tbi";
        
        OverlapPipe tt = new OverlapPipe(dataFile);
        //tt.tquery();
        
    
    }
}
