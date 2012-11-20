/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.aggregators;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class TokenAggregatorPipeTest {
    
    public TokenAggregatorPipeTest() {
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
     * Test of processNextStart method, of class TokenAggregatorPipe.
     */
    @Test
    public void testProcessNextStart() {
        Pipe p = new Pipeline(new TokenAggregatorPipe(">"));
        p.setStarts(Arrays.asList("> p1", "aaaaa", "> p2", "bbbbb", "ccccc", "> p3", "ddddd"));    
        for(int i=0;p.hasNext();i++){
            ArrayList<String> arr = (ArrayList<String>) p.next();
            if(i==0){
                assertEquals(2, arr.size());
                assertEquals("> p1", arr.get(0));
                assertEquals("aaaaa", arr.get(1));                
            }
            if(i==1){
                assertEquals(3, arr.size());
            }
            if(i==2){
                assertEquals("> p3", arr.get(0));
            }
        }
    }
}
