/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.DrainPipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.bioinformatics.GenbankPipe;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class BioJavaRichSequence2JSONTest {
    
    public BioJavaRichSequence2JSONTest() {
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
     * Test of processNextStart method, of class BioJavaRichSequence2JSON.
     */
    @Test
    public void testProcessNextStart() throws IOException {
        //assertEquals(expResult, result);
      
        System.out.println("processNextStart: BioJavaRichSequence2JSON");
        String current = new java.io.File( "." ).getCanonicalPath();
        String gbk = current + "/src/test/resources/testData/hs_ref_GRCh37.p9_chr17.gbs"; 
        
        
        System.out.println("Testing on Genbank file: " + gbk);
        
        //tell the pipe what type of features you want it to extract:
        String[] featureTypes = new String[1];
        featureTypes[0] = "gene";
        
        BioJavaRichSequence2JSON bjrs2tg = new BioJavaRichSequence2JSON("17", featureTypes);
        Pipe p = new Pipeline(new GenbankPipe(), bjrs2tg, new DrainPipe(), new PrintPipe());
        p.setStarts(Arrays.asList(gbk));
        for(int i=0; p.hasNext(); i++){
            p.next();
            //System.out.println(i);
        }
    }
}
