/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.bioinformatics.VCF2VariantPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class SameVariantPipeTest {
    
    public SameVariantPipeTest() {
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
    
    private String catalogFile = "src/test/resources/testData/sameVariantCatalog.tsv.gz";

    @Test
    public void testSomeMethod() {
        try {
            System.out.println("Process Next Start...");
            Pipe p = new Pipeline(new CatPipe(), new HistoryInPipe(), new VCF2VariantPipe(), new SameVariantPipe(catalogFile));
            p.setStarts(Arrays.asList("src/test/resources/testData/sameVariant.vcf"));
            while(p.hasNext()){
                History next = (History) p.next();
                
                System.out.println(next.get(8));
                System.out.println(next.get(9));
                System.out.println("****************************");
            }
        } catch (IOException ex) {
            Logger.getLogger(SameVariantPipeTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
