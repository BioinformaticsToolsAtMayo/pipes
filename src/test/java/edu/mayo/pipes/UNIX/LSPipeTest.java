/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

/**
 *
 * @author lauraquest
 */
public class LSPipeTest {
    
    public LSPipeTest() {
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
     * Test of processNextStart method, of class LSPipe.
     */
    @Test
    public void testProcessNextStart() {
        
        List<String> expected = new ArrayList<String>();
        expected.add("bar.txt");
        expected.add("baz.txt");
        expected.add("foo.txt");
        Collections.sort(expected);
        
        List<String> actual = new ArrayList<String>();        
        LSPipe ls = new LSPipe(false);
        Pipe<String,String> pipeline = new Pipeline<String,String>(ls);
        pipeline.setStarts(Arrays.asList("./src/test/resources/testData/lsFolderDontADDSTUFFINHERE"));
        while(pipeline.hasNext()) {
		  String s = pipeline.next();
		  actual.add(s);
        }
        Collections.sort(actual);
        
        assertEquals(expected, actual);        
    }
}
