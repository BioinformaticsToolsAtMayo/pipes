/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.junit.*;

import edu.mayo.pipes.DrainPipe;
import static org.junit.Assert.*;

/**
 *
 * @author lauraquest
 */
public class DrainPipeTest {
    
    public DrainPipeTest() {
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

    @Test
    public void testArray() {
        
    }
    
    @Test
    public void testCollection(){
        System.out.println("Test Drain Collection");
        ArrayList a = new ArrayList(Arrays.asList("aa", "ab", "ac"));
        ArrayList b = new ArrayList(Arrays.asList("ba", "bb", "bc"));
        ArrayList c = new ArrayList(Arrays.asList("ca", "cb", "cc"));
        ArrayList drainme = new ArrayList(Arrays.asList(a, b, c));
        DrainPipe<Collection, String> drain = new DrainPipe<Collection, String>();
        Pipe pipeline = new Pipeline(drain);
        pipeline.setStarts(drainme);
        for(int i=0; i<11 && pipeline.hasNext(); i++){
            String s = (String) pipeline.next();
            System.out.println(s);
            if(i==0){
                assertEquals("aa", s);
            }
            if(i==1){
                assertEquals("ab", s);
            }
            if(i==2){
                assertEquals("ac", s);
            }
            if(i==3){
                assertEquals("ba", s);
            }
            if(i==4){
                assertEquals("bb", s);
            }
            if(i==5){
                assertEquals("bc", s);
            }
            if(i==6){
                assertEquals("ca", s);
            }
            if(i==7){
                assertEquals("cb", s);
            }
            if(i==8){
                assertEquals("cc", s);
            }
            if(i==9){
                assertEquals("should never get here!", "");
            }
            assertTrue(i != 10);
        }
    }
}
