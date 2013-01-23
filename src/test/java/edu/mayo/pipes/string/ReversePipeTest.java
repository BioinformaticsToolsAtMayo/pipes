/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.string;

import com.tinkerpop.pipes.util.Pipeline;
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
public class ReversePipeTest {
    
    public ReversePipeTest() {
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
     * Test of processNextStart method, of class ReversePipe.
     */
    @Test
    public void testProcessNextStart() {
        Pipeline p = new Pipeline(new ReversePipe());
        p.setStarts(Arrays.asList("ABCDEFG"));
        String result = (String) p.next();
        assertEquals("GFEDCBA",result);
    }
}
