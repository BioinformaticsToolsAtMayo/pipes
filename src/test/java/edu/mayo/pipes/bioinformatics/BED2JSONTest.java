/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics;

import com.tinkerpop.pipes.transform.IdentityPipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.history.HistoryOutPipe;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import edu.mayo.pipes.UNIX.*;
import java.util.Arrays;

/**
 *
 * @author m102417
 */

public class BED2JSONTest {
    
    public BED2JSONTest() {
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
     * Test of processNextStart method, of class BED2JSONPipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("processNextStart");
        BED2JSONPipe bed2json = new BED2JSONPipe();
        Pipeline p = new Pipeline(
                                  new CatPipe(), 
                                  new HistoryInPipe(), 
                                  bed2json, 
                                  new HistoryOutPipe(),
                                  new PrintPipe()
                );
        p.setStarts(Arrays.asList("src/test/resources/testData/BED/complete.bed"));
        while(p.hasNext()){
            p.next();
        }
    }
}
