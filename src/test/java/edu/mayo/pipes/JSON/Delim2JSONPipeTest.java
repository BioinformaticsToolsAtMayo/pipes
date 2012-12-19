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
public class Delim2JSONPipeTest {
    
    public Delim2JSONPipeTest() {
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
        String delim = "pipe";
        String tab1 = "Rex|brown|12";
        String tab2 = "Simon|black|2.5";
        String tab3 = "Pillsbury|white|6";
        String[] meta = new String[3];
        meta[0] = "name";
        meta[1] = "color";
        meta[2] = "age";
        Delim2JSONPipe t2j = new Delim2JSONPipe(meta, delim);
        Pipeline p = new Pipeline(new HistoryInPipe(), t2j, new PrintPipe());
        p.setStarts(Arrays.asList(tab1, tab2, tab3));
        while(p.hasNext()){
            p.next();
        }
        
    }
}
