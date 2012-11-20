/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.aggregators;

import java.util.*;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author dquest
 */
public class ColumnAggregatorPipeTest {
    
    public ColumnAggregatorPipeTest() {
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
     * Test of processNextStart method, of class ColumnAggregatorPipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("ColumnAggregatorPipeTest");
        String[] s1 = {"White", "Lion", "Lives"};
        String[] s2 = {"The", "Lion", "is", "Rare"};
        String[] s3 = {"Hello", "Lion", "cub"};
        String[] s4 = {"Red", "Rabbit", "is", "Rare"};
        String[] s5 = {"Blue", "Rabbit", "is", "Common"};
        String[] s6 = {"Red", "Frog", "Lives"};
        ArrayList<String[]> l = new ArrayList<String[]>();
        l.add(s1); l.add(s2); l.add(s3); l.add(s4); l.add(s5); l.add(s6);
        ColumnAggregatorPipe agg = new ColumnAggregatorPipe(1);//note zero based
        agg.setStarts(l);
        Collection expResult = (Collection) agg.next();//s1,s2,s3
        assertEquals(expResult.size(), 3);
        Iterator<String[]> iterator = expResult.iterator();
        String[] s = iterator.next();
        assertEquals(s[0], s1[0]);//White
        s = iterator.next();
        assertEquals(s[3], s2[3]);//Rare
        expResult = (Collection) agg.next();//s4, s5
        assertEquals(2, expResult.size());
        Iterator iterator1 = expResult.iterator();
        assertEquals(s4[1], ((String[])iterator1.next())[1]);//Rabbit
        expResult = (Collection) agg.next();//s6
        assertEquals(1, expResult.size());
        Iterator iterator2 = expResult.iterator();
        assertEquals(s6[1], ((String[])iterator2.next())[1]);//Frog
    }
}
