/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import java.util.Arrays;
import org.junit.*;
import static org.junit.Assert.*;

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
        //System.out.println("lsPipeTest");
        String result = "";
        String expectedResult = "bar.txt\nbaz.txt\nfoo.txt\n";
        LSPipe ls = new LSPipe(false);
        Pipe<String,String> pipeline = new Pipeline<String,String>(ls);
        pipeline.setStarts(Arrays.asList("./src/test/resources/testData/lsFolderDontADDSTUFFINHERE"));
        while(pipeline.hasNext()) {
		  String s = pipeline.next();
		  result += s + "\n";	  
        }
        //System.out.println(result + "*");
        assertEquals(expectedResult, result);        
    }
}
