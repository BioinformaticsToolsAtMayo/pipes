/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import java.util.Arrays;
import java.util.List;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class SimpleDrillPipeTest {
    
    public SimpleDrillPipeTest() {
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
     * Test of processNextStart method, of class SimpleDrillPipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("Simple Drill Pipe Test");
        String s1 = "{\"type\":\"gene\",\"chr\":\"17\",\"strand\":\"+\",\"minBP\":41177258,\"maxBP\":41184058,\"gene\":\"RND2\",\"gene_synonym\":\"ARHN; RHO7; RhoN\",\"note\":\"Rho family GTPase 2; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"8153\",\"HGNC\":\"18315\",\"HPRD\":\"03332\",\"MIM\":\"601555\"}";
        String s2 = "{\"type\":\"gene\",\"chr\":\"17\",\"strand\":\"-\",\"minBP\":41196312,\"maxBP\":41277500,\"gene\":\"BRCA1\",\"gene_synonym\":\"BRCAI; BRCC1; BROVCA1; IRIS; PNCA4; PPP1R53; PSCP; RNF53\",\"note\":\"breast cancer 1, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"672\",\"HGNC\":\"1100\",\"HPRD\":\"00218\",\"MIM\":\"113705\"}";
        String s3 = "{\"type\":\"gene\",\"chr\":\"17\",\"strand\":\"+\",\"minBP\":41231278,\"maxBP\":41231833,\"gene\":\"RPL21P4\",\"gene_synonym\":\"RPL21_58_1548\",\"note\":\"ribosomal protein L21 pseudogene 4; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"140660\",\"HGNC\":\"17959\"}";

        String[] paths = new String[2];
        paths[0] = "gene";
        paths[1] = "minBP";
        Pipe p = new Pipeline(new SimpleDrillPipe(true, paths));
        p.setStarts(Arrays.asList(s1,s2,s3));
        for(int i=0; p.hasNext(); i++){
            List<String> drilled = (List<String>) p.next();
            for(int j=0; j<drilled.size(); j++){
                System.out.println(drilled.get(j));
            }
        }
//        SimpleDrillPipe instance = null;
//        List expResult = null;
//        List result = instance.processNextStart();
//        assertEquals(expResult, result);

    }
}
