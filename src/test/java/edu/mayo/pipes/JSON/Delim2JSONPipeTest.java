/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.history.History;
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
        String delim = "|";
        //My Dog Objects
        List<String> lists = Arrays.asList(
        		"Rex|brown|12",
        		"Simon|black|2.5",
        		"Pillsbury|white|6"
        		);
        String[] meta = { "name", "color", "age" };
        
        // Setup the pipes and start them
        Delim2JSONPipe delim2json = new Delim2JSONPipe(meta, delim);
        Pipeline p = new Pipeline(new HistoryInPipe(), delim2json, new PrintPipe());
        p.setStarts(lists);
        
        String[] expected = { 
        	"[Rex|brown|12,{\"name\":\"Rex\",\"color\":\"brown\",\"age\":12}]",
        	"[Simon|black|2.5,{\"name\":\"Simon\",\"color\":\"black\",\"age\":2.5}]",
        	"[Pillsbury|white|6,{\"name\":\"Pillsbury\",\"color\":\"white\",\"age\":6}]",
        };
        int expIdx = 0;
        while(p.hasNext()){
            History hist = (History)(p.next());
            assertEquals(expected[expIdx++], hist.toString());
        }
    }
}
