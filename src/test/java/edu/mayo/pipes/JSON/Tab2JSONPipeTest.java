/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.history.HistoryInPipe;
import java.util.Arrays;
import java.util.List;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class Tab2JSONPipeTest {
    
    public Tab2JSONPipeTest() {
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
     * Test of processNextStart method, of class Tab2JSONPipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("processNextStart");
        //My Dog Objects
        String tab1 = "Rex\tbrown\t12";
        String tab2 = "Simon\tblack\t2.5";
        String tab3 = "Pillsbury\twhite\t6";
        String[] meta = new String[3];
        meta[0] = "name";
        meta[1] = "color";
        meta[2] = "age";
        Tab2JSONPipe t2j = new Tab2JSONPipe(meta);
        Pipeline p = new Pipeline(new HistoryInPipe(), t2j, new PrintPipe());
        p.setStarts(Arrays.asList(tab1, tab2, tab3));
        while(p.hasNext()){
            p.next();
        }
        
    }
}
