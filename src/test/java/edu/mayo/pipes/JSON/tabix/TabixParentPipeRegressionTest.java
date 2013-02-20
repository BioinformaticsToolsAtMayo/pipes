/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.JSON.lookup.LookupPipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.history.HistoryInPipe;
import java.io.IOException;
import java.util.Arrays;
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
public class TabixParentPipeRegressionTest {
    
    public TabixParentPipeRegressionTest() {
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
    
    @Test
    public void testRegression() throws IOException{
        String dbIndexFile = "src/test/resources/testData/tabix/index/genes.HGNC.idx.h2.db";
        String catalog = "src/test/resources/testData/tabix/genes.tsv.bgz";
        String[] lines = {
            "1	123	456	18500", 
            "X	123	456	23270",
            "Y	123	456	.",	
            "22	123	456	38436",
            "22	123	456	6030"
        };
        Pipeline p = new Pipeline(
                new HistoryInPipe(),
                new LookupPipe(dbIndexFile,catalog), 
                new OverlapPipe(catalog),
                new PrintPipe()
                );
        p.setStarts(Arrays.asList(lines));
        for(int i=0; p.hasNext(); i++){
            p.next();
            //note matches stop after the 3rd to last or so :(
        }
    }

}
