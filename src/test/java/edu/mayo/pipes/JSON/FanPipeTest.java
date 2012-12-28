/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.UNIX.GrepEPipe;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.history.HistoryOutPipe;
import java.util.Arrays;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class FanPipeTest {
    
    public FanPipeTest() {
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

    @Test
    public void test() {
        //String s = "[\"A|ENSG00000260583|ENST00000567517|Transcript|upstream_gene_variant|||||||4432|||\",\"A|ENSG00000154719|ENST00000352957|Transcript|intron_variant||||||||||\",\"A|ENSG00000154719|ENST00000307301|Transcript|missense_variant|1043|1001|334|T\\/M|aCg\\/aTg|||tolerated(0.05)|benign(0.001)|\"]]";
        System.out.println("Testing Fan Pipe Basic Functionality...");        
        String input = "X\tY\tZ\t[\"A\",\"B\",\"C\"]";
        Pipe p = new Pipeline(new HistoryInPipe(), new FanPipe(), new HistoryOutPipe(), new GrepEPipe("^#.*") );
        p.setStarts(Arrays.asList(input));
        for(int i=1;p.hasNext();i++){
            String s = (String) p.next();
            if(i==1){
                assertEquals("X\tY\tZ\tA",s);
            }
            if(i==2){
                assertEquals("X\tY\tZ\tB",s);
            }
            if(i==3){
                assertEquals("X\tY\tZ\tC",s);
            }
        }
    }
}
