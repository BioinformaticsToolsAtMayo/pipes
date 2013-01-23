/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics.sequence;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.JSON.tabix.TabixReader;
import edu.mayo.pipes.JSON.tabix.TabixSearchPipe;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PickPipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.UNIX.CatGZPipe;
import edu.mayo.pipes.history.HistoryInPipe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class Bed2SequencePipeTest {
    
    public Bed2SequencePipeTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of processNextStart method, of class Bed2SequencePipe.
     */
    
    public String pos1 = "chr22\t1\t3";
    public String pos2 = "chr22\t1\t33";
    public String pos3 = "chr22\t3\t5";
    public String pos4 = "chr22\t1\t3";
    
    @Test
    public void testProcessNextStart() {
//        System.out.println("Bed2SequencePipeTest");
//        String sequence = "ATCCCGCGTACCC";
//        String header = "chr12";
//        Pipe p = new Pipeline(
//                new SplitPipe("\t"),
//                new Bed2SequencePipe(header, sequence),
//                new PickPipe(3));
//        p.setStarts(Arrays.asList(pos1, pos2, pos3));
//        String next = (String) p.next();
//        assertEquals("ATC", next);
//        next = (String) p.next();
//        assertEquals("ATCCCGCGTACCC", next);
//                next = (String) p.next();
//        assertEquals("CCC", next);

    }
    
    String tabixGenome = "src/test/resources/testData/chr22.fa.tsv.bgz";
    
    String p1 = "22	16449050	16449050"; //G
    String p2 = "22	17072016	17072016"; //G
    String p3 = "22	17072747	17072747"; //T
    String p4 = "22	17582776	17582776"; //C
    String p5 = "22	18899016	18899016"; //G
    @Test
    public void extractSequencePipeline() throws IOException{
        System.out.println("Extract Sequence Pipeline Test.");
        Pipe p = new Pipeline(new HistoryInPipe(), 
                              new Bed2SequencePipe(tabixGenome),
                              new PrintPipe());
        p.setStarts(Arrays.asList(p1,p2,p3,p4,p5));
        for(int i=0; p.hasNext(); i++){
            ArrayList al = (ArrayList) p.next();
            if(i==0){
                assertEquals("G",al.get(3));
            }
            if(i==1){
                assertEquals("G",al.get(3));
            }
            if(i==2){
                assertEquals("T",al.get(3));
            }
            if(i==3){
                assertEquals("C",al.get(3));
            }
            if(i==4){
                assertEquals("G",al.get(3));
            }
        }
    }
    
    @Test
    public void testTQuery() throws Exception {
        System.out.println("Test TQuery");
        String record = "";
        String r1 = "22\t16449021\t16449090\tCAGCCAAATGAGACCCACAGGTAGAGAAGGCCTTATGTCTCCCAGTGCTTGAAGGCATACCCAACATAGC";
        String query = "22:16449050-16449050";
        TabixSearchPipe op = new TabixSearchPipe(tabixGenome);
        TabixReader.Iterator records = op.tquery(query);
        for(int i=1;(record = records.next()) != null; i++){
            //System.out.println(record);
            if(i==1){
                assertEquals(r1,record);
            }
        }
    }

    

}
