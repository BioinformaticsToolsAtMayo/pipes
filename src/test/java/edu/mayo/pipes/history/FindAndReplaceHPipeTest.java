/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.history;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.SplitPipe;
import java.util.Arrays;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class FindAndReplaceHPipeTest {
    
    public FindAndReplaceHPipeTest() {
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
     * Test of processNextStart method, of class FindAndReplaceHPipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("processNextStart");
        String s1 = "Hello\tmine\tname\tis\tJoe"; //length = 5
        FindAndReplaceHPipe findReplace = new FindAndReplaceHPipe(2,"mine","my");
        Pipe p = new Pipeline(new HistoryInPipe(), findReplace, new HistoryOutPipe());
        p.setStarts(Arrays.asList(s1));
        p.next();//strip the stupid header
        String next = (String) p.next();
        assertEquals("Hello\tmy\tname\tis\tJoe", next);
    }
}
