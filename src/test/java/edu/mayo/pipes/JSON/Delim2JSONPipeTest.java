/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import java.util.Arrays;
import java.util.List;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class Delim2JSONPipeTest {
    
    public Delim2JSONPipeTest() {
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
     * Test of processNextStart method, of class Tab2JSONPipe.
     */
    @Test
    public void testSimple() {
        String delim = "pipe";
        //My Dog Objects
        List<String> lists = Arrays.asList(
        		"Rex|brown|12",
        		"Simon|black|2.5",
        		"Pillsbury|white|6"
        		);
        String[] meta = { "name", "color", "age" };
        
        // Setup the pipes and start them
        Delim2JSONPipe delim2json = new Delim2JSONPipe(meta, delim);
        Pipeline p = new Pipeline(new HistoryInPipe(), delim2json, new PrintPipe());
        p.setStarts(lists);
        
        String[] expected = { 
        	"[Rex|brown|12, {\"name\":\"Rex\",\"color\":\"brown\",\"age\":12}]",
        	"[Simon|black|2.5, {\"name\":\"Simon\",\"color\":\"black\",\"age\":2.5}]",
        	"[Pillsbury|white|6, {\"name\":\"Pillsbury\",\"color\":\"white\",\"age\":6}]",
        };
        int expIdx = 0;
        while(p.hasNext()){
            History hist = (History)(p.next());
            assertEquals(expected[expIdx++], hist.toString());
        }
    }

    @Test
    public void testSingleRow() {
        String delim = "pipe";
        //My Dog Objects
        List<String> lists = Arrays.asList(
        		"Rex|brown|12"
        		);
        String[] meta = { "name", "color", "age" };
        
        // Setup the pipes and start them
        Delim2JSONPipe delim2json = new Delim2JSONPipe(meta, delim);
        Pipeline p = new Pipeline(new HistoryInPipe(), delim2json, new PrintPipe());
        p.setStarts(lists);
        
        String[] expected = { 
        	"[Rex|brown|12, {\"name\":\"Rex\",\"color\":\"brown\",\"age\":12}]"
        };
        int expIdx = 0;
        while(p.hasNext()){
            History hist = (History)(p.next());
            assertEquals(expected[expIdx++], hist.toString());
        }
    }

    /** If we have a single dot in the column (meaning data was missing), 
     *  then we should return an empty JSON object */
    @Test
    public void dot() {
        String delim = "pipe";
        List<String> lists = Arrays.asList(
        		"."
        		);
        String[] meta = { "name" };
        
        // Setup the pipes and start them
        Delim2JSONPipe delim2json = new Delim2JSONPipe(meta, delim);
        Pipeline p = new Pipeline(new HistoryInPipe(), delim2json, new PrintPipe());
        p.setStarts(lists);
        
        String[] expected = { 
        	"[., {}]",
        };
        int expIdx = 0;
        while(p.hasNext()){
            History hist = (History)(p.next());
            assertEquals(expected[expIdx++], hist.toString());
        }
    }
    
    @Test
    public void testVepExample() {
        List<String> list = Arrays.asList(
            "A|ENSG00000260583|ENST00000567517|Transcript|upstream_gene_variant|||||||4432|||",
            "C|ENSG00000154719|ENST00000352957|Transcript|synonymous_variant|915|873|291|P|ccA/ccG|||||",
            "C|ENSG00000154719|ENST00000352957|Transcript|missense_variant|293|251|84|N/S|aAc/aGc|||tolerated(0.08)|possibly_damaging(0.463)|"
        );
        
        String[] headers = {"Allele", 
                    "Gene", 
                    "Feature",
                    "Feature_type",
                    "Consequence",
                    "cDNA_position",
                    "CDS_position",
                    "Protein_position",
                    "Amino_acids",
                    "Codons",
                    "Existing_variation",
                    "DISTANCE",
                    "SIFT",
                    "PolyPhen",
                    "CELL_TYPE"
        };
        Delim2JSONPipe delim2json = new Delim2JSONPipe(-1, false, headers, "pipe");
        Pipeline p = new Pipeline(new HistoryInPipe(), delim2json, new PrintPipe());
        p.setStarts(list);
        String[] expected = { 
        	"{\"Allele\":\"A\",\"Gene\":\"ENSG00000260583\",\"Feature\":\"ENST00000567517\",\"Feature_type\":\"Transcript\",\"Consequence\":\"upstream_gene_variant\",\"cDNA_position\":\"\",\"CDS_position\":\"\",\"Protein_position\":\"\",\"Amino_acids\":\"\",\"Codons\":\"\",\"Existing_variation\":\"\",\"DISTANCE\":4432}",
                "{\"Allele\":\"C\",\"Gene\":\"ENSG00000154719\",\"Feature\":\"ENST00000352957\",\"Feature_type\":\"Transcript\",\"Consequence\":\"synonymous_variant\",\"cDNA_position\":915,\"CDS_position\":873,\"Protein_position\":291,\"Amino_acids\":\"P\",\"Codons\":\"ccA/ccG\"}",
                "{\"Allele\":\"C\",\"Gene\":\"ENSG00000154719\",\"Feature\":\"ENST00000352957\",\"Feature_type\":\"Transcript\",\"Consequence\":\"missense_variant\",\"cDNA_position\":293,\"CDS_position\":251,\"Protein_position\":84,\"Amino_acids\":\"N/S\",\"Codons\":\"aAc/aGc\",\"Existing_variation\":\"\",\"DISTANCE\":\"\",\"SIFT\":\"tolerated(0.08)\",\"PolyPhen\":\"possibly_damaging(0.463)\"}"

        };
        int expIdx = 0;
        for(;p.hasNext();expIdx++){
            History hist = (History)(p.next());
            //System.out.println(hist.toString());
            //System.out.println(expected[expIdx]);
            assertEquals(expected[expIdx], hist.get(0).toString());
        }
        
    }
}
