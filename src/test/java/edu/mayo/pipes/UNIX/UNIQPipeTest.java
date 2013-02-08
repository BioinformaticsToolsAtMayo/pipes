/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.mayo.pipes.util.PipeTestUtils;

/**
 *
 * @author m102417
 */
public class UNIQPipeTest {
    
    public UNIQPipeTest() {
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
     * Test of processNextStart method, of class UNIQPipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("uniq pipe test");
        UNIQPipe uniquePipe = new UNIQPipe();
        uniquePipe.setStarts(Arrays.asList("do", "you", "like", "my", "hat", "no", "I", "do", "not", "like", "your", "hat"));
        List<String> expected = Arrays.asList("not", "no", "your", "you", "do", "like", "I", "my", "hat");
        List<String> actual = PipeTestUtils.getResults(uniquePipe);
        PipeTestUtils.assertListsEqual(expected, actual);
    }
    
}
