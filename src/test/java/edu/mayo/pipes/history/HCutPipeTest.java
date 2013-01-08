/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.history;

import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrintPipe;
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
public class HCutPipeTest {
    
    public HCutPipeTest() {
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
     * Test of processNextStart method, of class HCutPipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("Test HCutPipe");
        int[] cc = {2,4};
        String s1 = "A\tB\tC\tX\tW";
        String s2 = "D\tE\tF\tY\tW";
        Pipeline p = new Pipeline(new HistoryInPipe(), 
                                  new HCutPipe(cc), 
                                  //new MergePipe("\t", false), 
                                  new HistoryOutPipe(),
                                  new PrintPipe());
        p.setStarts(Arrays.asList(s1, s2));
        for(int i=0;p.hasNext();i++){
            String s = (String) p.next();
            if(i==0){
                assertEquals("#UNKNOWN_1\tUNKNOWN_3\tUNKNOWN_5", s);
            }
            if(i==1){
                assertEquals("A\tC\tW", s);
            }
            if(i==2){
                assertEquals("D\tF\tW", s);
            }
        }

    }
}
