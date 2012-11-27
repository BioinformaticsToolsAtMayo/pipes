/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import com.google.gson.JsonObject;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.DrainPipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.bioinformatics.GenbankPipe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.*;
import static org.junit.Assert.*;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;

/**
 *
 * @author m102417
 */
public class BioJavaRichSequence2JSONTest {
    
    public BioJavaRichSequence2JSONTest() {
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
     * Test of processNextStart method, of class BioJavaRichSequence2JSON.
     */
    //@Test
    public void testProcessNextStart() throws IOException {
        //assertEquals(expResult, result);
      
        System.out.println("processNextStart: BioJavaRichSequence2JSON");
        String current = new java.io.File( "." ).getCanonicalPath();
        String gbk = current + "/src/test/resources/testData/hs_ref_GRCh37.p9_chr17.gbs"; 
                
        System.out.println("Testing on Genbank file: " + gbk);
        
        //tell the pipe what type of features you want it to extract:
        String[] featureTypes = new String[3];
        featureTypes[0] = "gene";
        featureTypes[1] = "mRNA";
        featureTypes[2] = "CDS";        
        
        BioJavaRichSequence2JSON bjrs2tg = new BioJavaRichSequence2JSON("17", featureTypes);
        Pipe p = new Pipeline(new GenbankPipe(), bjrs2tg, new DrainPipe(), new PrintPipe());
        p.setStarts(Arrays.asList(gbk));
        for(int i=0; p.hasNext(); i++){
            p.next();
            //System.out.println(i);
        }
    }
    
    @Test
    public void testGene() throws Exception {
    	/*** SAMPLE LINE FOR BRAC1 from raw data file ***
    	gene            complement(41196312..41277500)
        /gene="BRCA1"
        /gene_synonym="BRCAI; BRCC1; BROVCA1; IRIS; PNCA4;
        PPP1R53; PSCP; RNF53"
        /note="breast cancer 1, early onset; Derived by automated
        computational analysis using gene prediction method:
        BestRefseq."
        /db_xref="GeneID:672"
        /db_xref="HGNC:1100"
        /db_xref="HPRD:00218"
        /db_xref="MIM:113705"
		**/

    	System.out.println("BioJavaRichSequence2JSONTest: Gene test (BRCA1)...");
        String current = new java.io.File( "." ).getCanonicalPath();
        String gbk = current + "/src/test/resources/testData/hs_ref_GRCh37.p9_chr17.gbs"; 
                
        System.out.println("Testing on Genbank file: " + gbk);
        
        //tell the pipe what type of features you want it to extract:
        String[] featureTypes = new String[1];
        featureTypes[0] = "gene";
        
        BioJavaRichSequence2JSON bjrs2tg = new BioJavaRichSequence2JSON("17", featureTypes);
        Pipe p = new Pipeline(new GenbankPipe(), bjrs2tg, new DrainPipe());
        p.setStarts(Arrays.asList(gbk));

        String resultJson = "";
        JsonPath jsonPath = null;
        boolean hasGene=false, hasGeneId=false, hasMinBp=false, hasMaxBp=false; 
        for(int i=0; p.hasNext(); i++){
            resultJson = (String)p.next();
            //System.out.println(resultJson);
            
            // Test Gene            
            jsonPath = JsonPath.compile("gene");
			if (jsonPath.read(resultJson).toString().equals("BRCA1")) {
				hasGene = true;
			}

            // Test GeneID            
            jsonPath = JsonPath.compile("GeneID");
			if (jsonPath.read(resultJson).toString().equals("672")) {
				hasGeneId = true;
			}
			
            // Test MinBp            
            jsonPath = JsonPath.compile("minBP");
			if (jsonPath.read(resultJson).toString().equals("41196312")) {
				hasMinBp = true;
			}

            // Test MaxBp            
            jsonPath = JsonPath.compile("maxBP");
			if (jsonPath.read(resultJson).toString().equals("41277500")) {
				hasMaxBp = true;
			}


        }    	
        //System.out.println(Arrays.toString(values.toArray()));        
    	assertTrue(hasGene);
    	assertTrue(hasGeneId);
    	assertTrue(hasMinBp);
    	assertTrue(hasMaxBp);
    }
}