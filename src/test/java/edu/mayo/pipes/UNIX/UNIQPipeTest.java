/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import edu.mayo.pipes.UNIX.*;

import java.util.Arrays;
import org.junit.*;
import static org.junit.Assert.*;

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
        UNIQPipe uniq = new UNIQPipe();
        uniq.setStarts(Arrays.asList("do", "you", "like", "my", "hat", "no", "I", "do", "not", "like", "your", "hat"));
        while(uniq.hasNext()){
            System.out.println(uniq.next());
        }
        
    }
}
