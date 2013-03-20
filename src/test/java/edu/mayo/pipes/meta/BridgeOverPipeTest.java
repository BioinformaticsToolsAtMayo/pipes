/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.meta;

import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.IdentityPipe;
import com.tinkerpop.pipes.transform.TransformFunctionPipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.pipeFunctions.StitchPipeFunction;
import edu.mayo.pipes.sideEffect.CachePipe;
import java.util.ArrayList;
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
public class BridgeOverPipeTest {
    
    public BridgeOverPipeTest() {
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
        
    public final String s1 = "A\tB\tC";
    public final String s2 = "E\tF\tG";
    
    /**
     * Test of processNextStart method, of class BridgeOverPipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("processNextStart");
        String appendMe = "D";
        Pipeline superviseMe = new Pipeline(new TransformFunctionPipe(new SomeLogic(appendMe)));
        BridgeOverPipe sp = new BridgeOverPipe(superviseMe,  new Chop(), new UnChop());
        Pipeline p = new Pipeline(
                new SplitPipe("\t"),
                new PrintPipe(),
                sp, //the BridgeOverPipe
                new PrintPipe()
        );
        p.setStarts(Arrays.asList(s1,s2));
        for(int i=0; p.hasNext(); i++){
            ArrayList<String> r = (ArrayList<String>) p.next();
            if(i==0){
                assertEquals("A", r.get(0));
                assertEquals("B", r.get(1));
                assertEquals("C", r.get(2));
                assertEquals("D", r.get(3));
            }
            if(i==1){
                assertEquals("E", r.get(0));
                assertEquals("F", r.get(1));
                assertEquals("G", r.get(2));
                assertEquals("D", r.get(3));
            }
        }
    }
    
    
    public class Chop implements PipeFunction<ArrayList<String>,ArrayList<String>> {
        /**
         * Split the input list a into two parts, ret and a, where a is the original
         * contents of the list and ret is some truncated partial list.  List a will be
         * cached for use in the StitchPipeFunction later in the pattern.
         * @param a
         * @return ret - the truncated partial list.
         */
        @Override
        public ArrayList<String> compute(ArrayList<String> a) {
            ArrayList<String> ret = new ArrayList<String>();
            ret.add(a.get(0));
            System.out.println("chop");
            return ret;
        }
    }
    
    public class SomeLogic implements PipeFunction<ArrayList<String>,ArrayList<String>> {
        private String abba;
        public SomeLogic(String appendMe){
            abba = appendMe;
        }

        @Override
        public ArrayList<String> compute(ArrayList<String> a) {
            System.out.println("logic");
            a.add(abba);
            return a;
        }
        
    }
    
    public class UnChop implements StitchPipeFunction<ArrayList<String>,ArrayList<String>,ArrayList<String>> {
        /**
         * Merge the two lists back together using whatever logic is needed (logic goes int the compute() method).
         * @param a - the array list coming out of the pipe
         * @param b - the array list before it went into the pipe
         * @return 
         */
        @Override
        public ArrayList<String> compute(ArrayList<String> a, ArrayList<String> b) {
            System.out.println("unchop");
            String r = a.get(a.size()-1);
            b.add(r);
            return b;
        }
    }
    
    
    /**
     * Test of getPipes method, of class BridgeOverPipe.
     */
    @Test
    public void testGetPipes() {
        System.out.println("getPipes");      
        BridgeOverPipe sp = new BridgeOverPipe(
                new Pipeline(new PrintPipe()),
                new Chop(),
                new UnChop()
                );
        List result = sp.getPipes();
        assertEquals(3, result.size());

    }
    

}
