/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.history;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.util.PipeTestUtils;

import java.util.Arrays;
import java.util.List;

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
public class SquishPipeTest {
    
    public SquishPipeTest() {
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
     * Test of processNextStart method, of class SquishPipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("processNextStart");
        String s1 = "A\tB\tC";
        String s2 = "A\tB\tC";
        String s3 = "A\tB\tD";
        String s4 = "A\tB\tE";
        String s5 = "A\tB\tE";
        SquishPipe squish = new SquishPipe(2);
        Pipe p = new Pipeline(new HistoryInPipe(), squish, new MergePipe("\t"));
        List<String> expected = Arrays.asList(
        		"A\tB\tC",
        		"A\tB\tD",
        		"A\tB\tE" );
        p.setStarts(Arrays.asList(s1,s2,s3,s4,s5));
        List<String> actual = PipeTestUtils.getResults(p);
        PipeTestUtils.assertListsEqual(expected, actual);
    }
}
