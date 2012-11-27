/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

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
public class MergePipeTest {
    
    public MergePipeTest() {
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
     * Test of processNextStart method, of class MergePipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("simple Test Merge Pipe");
        String result = "A fat brown fox sat on a log";
        ArrayList<String> mergeMe = new ArrayList<String>();
        mergeMe.add("A");
        mergeMe.add("fat");
        mergeMe.add("brown");
        mergeMe.add("fox");
        mergeMe.add("sat");
        mergeMe.add("on");
        mergeMe.add("a");
        mergeMe.add("log");
        Pipe p = new Pipeline(new MergePipe(" "));
        p.setStarts(Arrays.asList(mergeMe));
        for(int i=0; p.hasNext(); i++){
            String merged = (String) p.next();
            //System.out.println(merged);
            assertEquals(result, merged);
        }
    }
    
    @Test
    public void testOneElement() {
        System.out.println("simple Test Merge Pipe One Element Test");
        String result = "brown";
        ArrayList<String> mergeMe = new ArrayList<String>();
        mergeMe.add("brown");
        Pipe p = new Pipeline(new MergePipe(" "));
        p.setStarts(Arrays.asList(mergeMe));
        for(int i=0; p.hasNext(); i++){
            String merged = (String) p.next();
            //System.out.println(merged);
            assertEquals(result, merged);
        }
    }
}
