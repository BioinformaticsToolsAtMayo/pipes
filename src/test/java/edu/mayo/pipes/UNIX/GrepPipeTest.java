/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import edu.mayo.pipes.UNIX.GrepPipe;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.transform.IdentityPipe;
import com.tinkerpop.pipes.util.Pipeline;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author dquest
 */
public class GrepPipeTest {
    
    public GrepPipeTest() {
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
     * Test of processNextStart method, of class grepPipe.
     */
    @Test
    public void testProcessNextStart() {
        //System.out.println("processNextStart");
        LinkedList<String> result = new LinkedList<String>();
        GrepPipe grep_pipe = new GrepPipe(".*a.*");
        IdentityPipe idp = new IdentityPipe();
        Pipe<String,String> pipeline = new Pipeline<String,String>(grep_pipe);
        pipeline.setStarts(Arrays.asList("Dan", "Vita", "Mike", "Pat"));
        while(pipeline.hasNext()) {
            String name = pipeline.next();
            //System.out.println(name);
            result.add(name);	  
        }
        List<String> expected = Arrays.asList("Dan", "Vita", "Pat");
        assertEquals(3, result.size());
        for( int i = 0; i< expected.size(); i++){
            //System.out.println("Result " + i + " [" + expected.get(i) + ":" + result.get(i) + "]");
            assertEquals(expected.get(i), result.get(i));
        }
    }
}
