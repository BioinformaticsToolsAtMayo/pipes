/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.UNIX.GrepEPipe;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.history.HistoryOutPipe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 *         
 * This is used as a follow-up to a drill pipe, which will search for the CSQ key within the INFO column.
 * CSQ=A|B|C,1|2|3	=>  [A|B|C,1|2|3]
 * CSQ=A|B|C		=>  [A|B|C]
 * CSQ=				=>	(Not a valid scenario)
 * "" (no CSQ field)=>  .
 */
public class FanPipeTest {
    
    public FanPipeTest() {
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

    /** The fanout matched three records, so three copies of the original line are returned
     *  with a different value in the last column for all three lines     */
    @Test
    public void threeMatches() {
        //String s = "[\"A|ENSG00000260583|ENST00000567517|Transcript|upstream_gene_variant|||||||4432|||\",\"A|ENSG00000154719|ENST00000352957|Transcript|intron_variant||||||||||\",\"A|ENSG00000154719|ENST00000307301|Transcript|missense_variant|1043|1001|334|T\\/M|aCg\\/aTg|||tolerated(0.05)|benign(0.001)|\"]]";
        System.out.println("Testing Fan Pipe Basic Functionality...");        
        String input = "X\tY\tZ\t[\"A\",\"B\",\"C\"]";
        Pipe p = new Pipeline(new HistoryInPipe(), new FanPipe(), new HistoryOutPipe(), new GrepEPipe("^#.*") );
        p.setStarts(Arrays.asList(input));
        assertArrayEquals(
        		new String[] { "X\tY\tZ\tA",  "X\tY\tZ\tB",  "X\tY\tZ\tC" },
        		getAllPipeOutput(p) );
    }

    

    /** The fanout matched three NUMERIC records, so three copies of the original line are returned
     *  with a different value in the last column for all three lines     */
    @Test
    public void threeMatchesNumeric() {
        String input = "X\tY\tZ\t[1,2,3]";
        Pipe p = new Pipeline(new HistoryInPipe(), new FanPipe(), new HistoryOutPipe(), new GrepEPipe("^#.*") );
        p.setStarts(Arrays.asList(input));
        // TODO: We may have to account for this later, but 
        
        System.out.println("NOT DOING CURRENTLY");
        assertArrayEquals(
        		new String[] { "X\tY\tZ\t1.0",  "X\tY\tZ\t2.0",  "X\tY\tZ\t3.0" },
        		getAllPipeOutput(p) );
    }

    //===============================================
    // NOT A VALID CASE
    //===============================================
//    /** The original line contains a blank list, so only the original line with a "." in the last column is returned */
//    @Test
//    public void noMatchEmptyList() {
//        String input = "X\tY\tZ\t[]";
//        Pipe p = new Pipeline(new HistoryInPipe(), new FanPipe(), new HistoryOutPipe(), new GrepEPipe("^#.*") );
//        p.setStarts(Arrays.asList(input));
//        assertArrayEquals(
//        		new String[] { "X\tY\tZ\t." },
//        		getAllPipeOutput(p) );
//    }

    
    //===============================================
    // NOT A VALID CASE
    //===============================================
//    /** The fanout matched one record (not in a list), so the original line is returned
//     *  with some value in the last column     */
//    @Test
//    public void oneMatchNotInList() {
//        String input = "X\tY\tZ\t\"A\"";
//        Pipe p = new Pipeline(new HistoryInPipe(), new FanPipe(), new HistoryOutPipe(), new GrepEPipe("^#.*") );
//        p.setStarts(Arrays.asList(input));
//        assertArrayEquals(
//        		new String[] { "X\tY\tZ\tA" },
//        		getAllPipeOutput(p) );
//    }

    /** The fanout matched one record that is in a list, so the original line is returned
     *  with some value in the last column     */
    @Test
    public void oneMatchInList() {
        String input = "X\tY\tZ\t[\"A\"]";
        Pipe p = new Pipeline(new HistoryInPipe(), new FanPipe(), new HistoryOutPipe(), new GrepEPipe("^#.*") );
        p.setStarts(Arrays.asList(input));
        assertArrayEquals(
        		new String[] { "X\tY\tZ\tA" },
        		getAllPipeOutput(p) );
    }
    

    /** The original line contains a blank element in the column (represented as a "."),
     *  so only the original line with a "." in the last column is returned */
    @Test
    public void noMatchDot() {
        String input = "X\tY\tZ\t.";
        Pipe p = new Pipeline(new HistoryInPipe(), new FanPipe(), new HistoryOutPipe(), new GrepEPipe("^#.*") );
        p.setStarts(Arrays.asList(input));
        assertArrayEquals(
        		new String[] { "X\tY\tZ\t." },
        		getAllPipeOutput(p) );
    }


    
    
    /** NOTE: Assumes the output of the pipe is a string */
    private String[] getAllPipeOutput(Pipe p) {
    	ArrayList<String> output = new ArrayList<String>();
    	while(p.hasNext()) {
    		output.add((String)p.next());
    	}
    	return output.toArray(new String[output.size()]);
    }
    
}
