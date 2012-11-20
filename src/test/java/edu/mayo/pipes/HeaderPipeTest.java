/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author lauraquest
 */
public class HeaderPipeTest {
    
    public HeaderPipeTest() {
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
    public void testOneLineHeader() {
        System.out.println("OneLineHeaderPipeTest");
        List<String> l = Arrays.asList("line1", "line2", "line3", "line4");
        HeaderPipe hp = new HeaderPipe(1);
        hp.setStarts(l);
        String record2 = (String) hp.next();
        
        assertEquals(l.get(1), record2);
        assertEquals(l.get(0)+"\n", hp.getHeader());
        String record3 = (String) hp.next();
        assertEquals(l.get(2), record3);
        String record4 = (String) hp.next();
        assertEquals(l.get(3), record4);
        
    }
    
    @Test
    public void testMultiLineHeader() {
        System.out.println("MultiLineHeaderPipeTest");
        List<String> l = Arrays.asList("line1", "line2", "line3", "line4", "line5");
        HeaderPipe hp = new HeaderPipe(3);
        hp.setStarts(l);
        String record2 = (String) hp.next();
        String header = l.get(0) + "\n" + l.get(1) + "\n" + l.get(2) + "\n";
        
        assertEquals(l.get(3), record2);
        assertEquals(header, hp.getHeader());
        
    }
}
