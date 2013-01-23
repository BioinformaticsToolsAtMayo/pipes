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
public class ComplementPipeTest {
    
    public ComplementPipeTest() {
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
     * Test of processNextStart method, of class ComplementPipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("complementPipeTest");
        Pipeline p = new Pipeline( new ComplementPipe() );
        p.setStarts(Arrays.asList("AaCcGgTtNnXxUu"));
        String expResult = "TtGgCcAaNnXxAa";
        String result = (String) p.next();
        assertEquals(expResult, result);
    }
}
