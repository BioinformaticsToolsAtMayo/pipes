/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import com.jayway.jsonpath.JsonPath;
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

    
    /**
     * POSSIBLE TEST CASES
     * 1. Variant with no match.
     * 2. Variant with 1 match.
     * 3. Variant with more than 1 matches.
     * 4. Variant with position that is same, but not matched
     * 5. Variant with Chrom + Pos that is same, but not matched
     * 6. Variant that matched on rsId + Chr + Pos, but if Flag is ON, then it shouldn't match
     * 7. Variant that matched on ref/alt + Chr + Pos, but if Flag is ON, then it shouldn't match
     * 8. If both Flags are ON.. match either way "OR" logic
     * 
     */
    
    @Test
    public void testSomeMethod() {
//        try {
//            System.out.println("Process Next Start...");
//            Pipe p = new Pipeline(new CatPipe(), new HistoryInPipe(), new VCF2VariantPipe(), new SameVariantPipe(catalogFile));
//            p.setStarts(Arrays.asList("src/test/resources/testData/sameVariant.vcf"));
//            while(p.hasNext()){
//                History next = (History) p.next();
//                for(int i=0; i< next.size(); i++){
//                    System.out.println(next.get(i));
//                }
//                System.out.println("****************************");
//            }
//        } catch (IOException ex) {
//            Logger.getLogger(SameVariantPipeTest.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }
    
    
    /**
     * 1. Variant with no match.
     * @throws IOException
     */
    @Test
    public void testNoMatch() throws IOException{
        String variantThatDoesNotMatch = "20	24970000	rsFOOBAZ	T	C	.	.	.";
        Pipe same = new SameVariantPipe(catalogFile);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantThatDoesNotMatch));
        History next = (History) p.next();
        assertEquals("{}", next.get(9));
    }

    /**
     * 2. Variant with 1 match.
     * @throws IOException
     */
    @Test
    public void testOneMatch() throws IOException{
        String variantOneMatch = "21	26960070	rs116645811	G	A	.	.	.";
        Pipe same = new SameVariantPipe(catalogFile);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantOneMatch));
        
        int resultCount = 0;
        while(p.hasNext()) {
        	History next = (History) p.next();
        	resultCount++;
            //System.out.println(next.get(9));
            assertEquals("21", JsonPath.compile("CHROM").read(next.get(9)));
            assertEquals("26960070", JsonPath.compile("POS").read(next.get(9)));
            assertEquals("rs116645811", JsonPath.compile("ID").read(next.get(9)));
        }
        assertEquals(1, resultCount); //only one row need to be returned
    }

    /**
     * 3. Variant with multiple matches.
     * @throws IOException
     */
    @Test
    public void testMultipleMatch() throws IOException{
        String variantMultipleMatch = "21	26976144	rs116331755	A	G	.	.	.";
        Pipe same = new SameVariantPipe(catalogFile);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantMultipleMatch));

        int resultCount = 0;
        while(p.hasNext()){
            History next = (History) p.next();
        	resultCount++;
        	
        	// This loops goes thru 3 times, in each case, Chrom, Pos and ID must be the same. So this assert should not fail.
            assertEquals("21", JsonPath.compile("CHROM").read(next.get(9)));
            assertEquals("26976144", JsonPath.compile("POS").read(next.get(9)));
            assertEquals("rs116331755", JsonPath.compile("ID").read(next.get(9)));
        }
        assertEquals(3, resultCount); //only one row need to be returned
    }
}
