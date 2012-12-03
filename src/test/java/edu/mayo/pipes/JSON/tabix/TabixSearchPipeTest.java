/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.JSON.tabix.TabixReader.Iterator;
import edu.mayo.pipes.PrintPipe;
import java.io.IOException;
import java.util.Arrays;
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
    public void testProcessNextStart() throws IOException {
        String query = "{\"_landmark\":\"17\",\"_minBP\":41196312,\"_maxBP\":41300000\"}"; //5 results
        Pipe p = new Pipeline(new TabixSearchPipe(geneFile), new PrintPipe());
        p.setStarts(Arrays.asList(query));
        while(p.hasNext()){
            p.next();
        }
    }
    
    @Test
    public void testTQuery() throws IOException{
        System.out.println("Test TQuery");
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
    public void testQuery() throws IOException{
        System.out.println("Test Query Based on JSON input");
        String record = "";
        TabixSearchPipe op = new TabixSearchPipe(geneFile);
        String brca1 = "{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"-\",\"_minBP\":41196312,\"_maxBP\":41277500,\"gene\":\"BRCA1\",\"gene_synonym\":\"BRCAI; BRCC1; BROVCA1; IRIS; PNCA4; PPP1R53; PSCP; RNF53\",\"note\":\"breast cancer 1, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"672\",\"HGNC\":\"1100\",\"HPRD\":\"00218\",\"MIM\":\"113705\"}";
        Iterator records = op.query(brca1);
        for(int i=1;(record = records.next()) != null; i++){
            System.out.println("Record:"+record);
            String[] s = record.split("\t");
            assertEquals(4, s.length);
            assertEquals("17", s[0]);
            assertEquals("41196312", s[1]); 
            assertEquals("41277500", s[2]);
        }
    }     

}
