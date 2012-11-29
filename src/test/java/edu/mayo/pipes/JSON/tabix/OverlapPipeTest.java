/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import edu.mayo.pipes.JSON.tabix.TabixReader.Iterator;
import java.io.IOException;
import java.util.List;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class OverlapPipeTest {
    
    public OverlapPipeTest() {
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


    public String dataFile = "src/test/resources/testData/tabix/example.gff.gz";
    public String tabixIndexFile = "src/test/resources/testData/tabix/example.gff.gz.tbi";
    
    public void testTQuery() throws IOException{
        String record = "";
        String query = "abc123:7000-13000";
        OverlapPipe op = new OverlapPipe(dataFile);
        Iterator records = op.tquery(query);
        while((record = records.next()) != null){
            System.out.println(record);
        }
    }
    
    public void testQuery() throws IOException{
        System.out.println("Test Query");
        OverlapPipe op = new OverlapPipe(dataFile);
    }
    
    
    /**
     * Test of query method, of class OverlapPipe.
     */
    @Test
    public void testQuery_String() throws IOException {
        System.out.println("Test Query Based on JSON input");
        
        String json = "";
        OverlapPipe op = new OverlapPipe(dataFile);
        String record;
        Iterator records = op.query(json);        
        while((record = records.next()) != null){
            System.out.println(record);
        }

    }


    

    /**
     * Test of processNextStart method, of class Overlap.
     */
    @Test
    public void testProcessNextStart() throws IOException {
        System.out.println( "Tabix Test!" );
        

        
//        Pipe p = new Pipeline( new Overlap(dataFile) );
//        p.setStarts(Arrays.asList(""));
//        for(int i=0; p.hasNext(); i++){
//            p.next();
//        }
        
        //Overlap instance = null;
        //List expResult = null;
        //List result = instance.processNextStart();
        //assertEquals(expResult, result);
        
        
        //String dataFile = "/Users/m102417/tabixtest/example.gff.gz";
        //String tabixIndexFile = "/Users/m102417/tabixtest/example.gff.gz.tbi";
        
        OverlapPipe tt = new OverlapPipe(dataFile);
        //tt.tquery();
        
    
    }
}
