/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import org.biojava.bio.seq.Feature;
import org.biojava.bio.symbol.Symbol;
import org.biojava.bio.symbol.SymbolList;
import org.biojavax.bio.seq.RichSequence;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class GenbankPipeTest {
    
    public GenbankPipeTest() {
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
     * Test of parse method, of class GenbankPipe.
     */
    @Test
    public void testParse() throws IOException {
        System.out.println("GenbankPipe.testParse");
        String current = new java.io.File( "." ).getCanonicalPath();
        String gbk = current + "/src/test/resources/testData/example.gbk";
        System.out.println("Processing:"+gbk);
        Pipe p = new Pipeline(new GenbankPipe());
        p.setStarts(Arrays.asList(gbk));
        for(int i=1; p.hasNext(); i++){
            System.out.println(i);
            RichSequence rs = (RichSequence)p.next();
            //two of the same sequence...
            assertEquals("SCU49845", rs.getName());
            assertEquals("U49845", rs.getAccession());
            
            Iterator<Feature> features = rs.features();
            while(features.hasNext()){
                Feature f = features.next();
                assertEquals("GenBank", f.getSource());
                
                //System.out.println("Type: " + f.getType());//source, CDS, gene, 
                //System.out.println("Location: " + f.getLocation());
                //System.out.println("Parent Feature: " + f.getParent());//biojava:SCU49845/U49845.1
                
//                SymbolList symbols = f.getSymbols();
//                Iterator<Symbol> it = symbols.iterator();
//                while(it.hasNext()){
//                    Symbol next = it.next();
//                    System.out.println("\tqualName" + next.getName());
//                    System.out.println("\tqual" );
//                }
                //System.out.println("Symbols: " + f.getSymbols());
                
            }
        }
    }


}
