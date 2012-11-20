/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.aggregators;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.UNIX.CatGZPipe;

import java.util.*;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author dquest
 */
public class AggregatorPipeTest {
    
    public AggregatorPipeTest() {
    }

    /**
     * Test of processNextStart method, of class AggregatorPipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("AggregatorPipeTest");
        List<String> l = Arrays.asList("daniel", "john", "quest", "did", "this");
        AggregatorPipe agg = new AggregatorPipe(3);
        agg.setStarts(l);
        Collection expResult = (Collection) agg.next();
        assertEquals(expResult.size(), 3);
        Iterator<String> iterator = expResult.iterator();
        assertEquals(iterator.next(), "daniel");
        assertEquals(iterator.next(), "john");
        assertEquals(iterator.next(), "quest");
        expResult = (Collection) agg.next();
        iterator = expResult.iterator();
        assertEquals(expResult.size(), 2);
        assertEquals(iterator.next(), "did");
        assertEquals(iterator.next(), "this");
        
    }
    
       @Test
    public void test2() {
        System.out.println("AggregatorPipeTest2");
        List<String> l = Arrays.asList("daniel", "john", "quest", "did", "this");
        AggregatorPipe agg = new AggregatorPipe(6);
        agg.setStarts(l);
        Collection expResult = (Collection) agg.next();
        assertEquals(expResult.size(), 5);
//        Iterator<String> iterator = expResult.iterator();
//        assertEquals(iterator.next(), "daniel");
//        assertEquals(iterator.next(), "john");
//        assertEquals(iterator.next(), "quest");
//        expResult = (Collection) agg.next();
//        iterator = expResult.iterator();
//        assertEquals(expResult.size(), 2);
//        assertEquals(iterator.next(), "did");
//        assertEquals(iterator.next(), "this");        
    }
    
    
    @Test
    public void test3() {
        System.out.println("AggregatorPipeTest3");
        String result = "";
        Pipe<String,String> zip_pipe = new CatGZPipe("gzip");
        AggregatorPipe agg = new AggregatorPipe(4);
        Pipe<String,Collection> pipeline = new Pipeline<String,Collection>(zip_pipe, new PrintPipe(), agg);
        pipeline.setStarts(Arrays.asList("src/test/resources/testData/example.gff.gz", "src/test/resources/testData/example2.gff.gz"));
        
        while(pipeline.hasNext()) {
        	pipeline.next();
        }        
    }
    


}
