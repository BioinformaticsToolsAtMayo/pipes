/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.aggregators.AggregatorPipe;
import java.io.IOException;
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
public class ExecPipeTest {
    
    public ExecPipeTest() {
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
     * Test of processNextStart method, of class ExecPipe.
     */
    @Test
    public void testProcessNextStart() throws IOException, InterruptedException {
        System.out.println("processNextStart");
        String[] command = {"cat"};
        ExecPipe exe = new ExecPipe(command, true);
        Pipe p = new Pipeline(
                new AggregatorPipe(100),
                exe,
                new DrainPipe(),
                new PrintPipe()
                );
        List<String> asList = Arrays.asList("foo", "bar");
        p.setStarts(asList);
        for(int i=0;p.hasNext();i++){
            String result = (String) p.next();
            if(i==0){
                assertEquals("foo", result);
            }
            if(i==1){
                assertEquals("bar", result);
            }
        }
        exe.shutdown(); //MAKE SURE TO DO THIS EVERY TIME YOU USE AN EXECPIPE!!!
    }
    
        /**
     * Test of processNextStart method, of class ExecPipe.
     */
    @Test
    public void testGrep() throws IOException {
        System.out.println("test grep via exec pipe");
        String[] command = {"grep", "bar"};
        ExecPipe exe = new ExecPipe(command, true);
        Pipe p = new Pipeline(
                new AggregatorPipe(100),
                exe,
                new DrainPipe(),
                new PrintPipe()
                );
        List<String> asList = Arrays.asList("foo", "bar");
        p.setStarts(asList);
        for(int i=0;p.hasNext();i++){
            String result = (String) p.next();
            if(i==0){
                assertEquals("bar", result);
            }
        }
        //assertEquals(expResult, result);

    }


}
