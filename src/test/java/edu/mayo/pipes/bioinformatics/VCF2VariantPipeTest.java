/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.jsonpath.JsonPath;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.bioinformatics.vocab.Type;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;

/**
 *
 * @author m102417
 */
public class VCF2VariantPipeTest {
    
    public VCF2VariantPipeTest() {
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

//    @Test
//    public void test() {
//    	
//    	// pipes
//    	CatPipe			cat 	= new CatPipe();
//    	HistoryInPipe historyIn = new HistoryInPipe();
//        VCF2VariantPipe vcf 	= new VCF2VariantPipe();
//        
//        Pipe<String, History> pipeline = new Pipeline<String, History>
//        	(
//        		cat,		// read VCF line	--> String
//        		historyIn,	// String			--> history
//        		vcf			// history			--> add JSON to end of history
//        	);
//        pipeline.setStarts(Arrays.asList("/tmp/SangerPanelSnps.vcf"));
//
//        // grab 1st row of data
//        pipeline.hasNext();
//        History history = pipeline.next();
//        String json = history.get(history.size() - 1);
//    }    
    
    /**
     * Tests for empty/NULL columns
     */
    @Test
    public void testNullColumns() {
    	
    	List<String> vcfLines = new ArrayList<String>();
    	 
    		// VCF HEADER
    	vcfLines.add("##fileformat=VCFv4.0\n");
    	vcfLines.add("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n");
    				
    		// row #1 has NULL values for columns QUAL, FILTER, and INFO
    	vcfLines.add("2\t48010558\trs1042820\tC\tA\t\t\t\t\n");
    		
    		// row #2 has NULL values for columns QUAL, FILTER, and INFO
    	vcfLines.add("1\t45792936\t.\tT\tG\t\t\t\t\n");

    	// pipes
    	HistoryInPipe historyIn = new HistoryInPipe();
        VCF2VariantPipe vcf 	= new VCF2VariantPipe();
        
        Pipe<String, History> pipeline = new Pipeline<String, History>
        	(
        		historyIn,	// String			--> history
        		vcf			// history			--> add JSON to end of history
        	);
        pipeline.setStarts(vcfLines);

        // grab 1st row of data
        assertTrue(pipeline.hasNext());
        History history = pipeline.next();
        String json = history.get(history.size() - 1);
        
        // use JSON paths to drill out values and compare with expected
        assertEquals("2",			JsonPath.compile("CHROM").read(json));
        assertEquals("48010558",	JsonPath.compile("POS").read(json));
        assertEquals("rs1042820",	JsonPath.compile("ID").read(json));
        assertEquals("C",			JsonPath.compile("REF").read(json));
        assertEquals("A",			JsonPath.compile("ALT").read(json));
        assertEquals("",			JsonPath.compile("QUAL").read(json));
        assertEquals("",			JsonPath.compile("FILTER").read(json));
        assertEquals("{}",			JsonPath.compile("INFO").read(json).toString());
        assertEquals("rs1042820",	JsonPath.compile(CoreAttributes._id.toString()).read(json));
        assertEquals("2",			JsonPath.compile(CoreAttributes._landmark.toString()).read(json));
        assertEquals(48010558,		JsonPath.compile(CoreAttributes._minBP.toString()).read(json));
        assertEquals(48010558,		JsonPath.compile(CoreAttributes._maxBP.toString()).read(json));
        assertEquals("C",			JsonPath.compile(CoreAttributes._refAllele.toString()).read(json));
        assertEquals("A",			JsonPath.compile(CoreAttributes._altAlleles.toString()+"[0]").read(json));
        assertEquals(Type.VARIANT.toString(),	JsonPath.compile(CoreAttributes._type.toString()).read(json));
        
        // grab 2nd row of data only
        pipeline.hasNext();	    
        history = pipeline.next();
        json = history.get(history.size() - 1);    	
    }    
    
    /**
     * Test of processNextStart method, of class VCF2VariantPipe.
     */
    @Test
    public void testProcessNextStart() {
    	
    	// pipes
    	CatPipe			cat 	= new CatPipe();
    	HistoryInPipe historyIn = new HistoryInPipe();
        VCF2VariantPipe vcf 	= new VCF2VariantPipe();
        
        Pipe<String, History> pipeline = new Pipeline<String, History>
        	(
        		cat,		// read VCF line	--> String
        		historyIn,	// String			--> history
        		vcf			// history			--> add JSON to end of history
        	);
        pipeline.setStarts(Arrays.asList("src/test/resources/testData/vcf-format-4_0.vcf"));

        // grab 1st row of data
        pipeline.hasNext();
        History history = pipeline.next();
        String json = history.get(history.size() - 1);
        
        // use JSON paths to drill out values and compare with expected
        assertEquals("1",			JsonPath.compile("CHROM").read(json));
        assertEquals("10144",		JsonPath.compile("POS").read(json));
        assertEquals("rs144773400",	JsonPath.compile("ID").read(json));
        assertEquals("TA",			JsonPath.compile("REF").read(json));
        assertEquals("T",			JsonPath.compile("ALT").read(json));
        assertEquals("GOOD",		JsonPath.compile("QUAL").read(json));
        assertEquals("PASS",		JsonPath.compile("FILTER").read(json));
        assertEquals(true,			JsonPath.compile("INFO.MOCK_FLAG").read(json));
        assertEquals("A",			JsonPath.compile("INFO.MOCK_CHAR").read(json));
        assertEquals("foobar",		JsonPath.compile("INFO.MOCK_STR").read(json));
        assertEquals(3,				JsonPath.compile("INFO.MOCK_INTEGER").read(json));
        assertEquals(3.78,			JsonPath.compile("INFO.MOCK_FLOAT").read(json));
        assertEquals("rs144773400",	JsonPath.compile(CoreAttributes._id.toString()).read(json));
        assertEquals("1",			JsonPath.compile(CoreAttributes._landmark.toString()).read(json));
        assertEquals(10144,			JsonPath.compile(CoreAttributes._minBP.toString()).read(json));
        assertEquals(10145,			JsonPath.compile(CoreAttributes._maxBP.toString()).read(json));
        assertEquals("TA",			JsonPath.compile(CoreAttributes._refAllele.toString()).read(json));
        assertEquals("T",			JsonPath.compile(CoreAttributes._altAlleles.toString()+"[0]").read(json));
        assertEquals(Type.VARIANT.toString(),	JsonPath.compile(CoreAttributes._type.toString()).read(json));
        
        // grab 2nd row of data only
        pipeline.hasNext();	    
        history = pipeline.next();
        json = history.get(history.size() - 1);
        
        // use JSON paths to drill out values and compare with expected
        assertEquals("20",			JsonPath.compile("CHROM").read(json));
        assertEquals("9076",		JsonPath.compile("POS").read(json));
        assertEquals("fake_id",		JsonPath.compile("ID").read(json));
        assertEquals("AGAAA",		JsonPath.compile("REF").read(json));
        assertEquals("A",			JsonPath.compile("ALT").read(json));
        assertEquals("AVERAGE",		JsonPath.compile("QUAL").read(json));
        assertEquals("FAIL",		JsonPath.compile("FILTER").read(json));
        assertEquals("foobar1",		JsonPath.compile("INFO.MOCK_STR_MULTI[0]").read(json));
        assertEquals("foobar2",		JsonPath.compile("INFO.MOCK_STR_MULTI[1]").read(json));
        assertEquals(1,				JsonPath.compile("INFO.MOCK_INTEGER_MULTI[0]").read(json));
        assertEquals(2,				JsonPath.compile("INFO.MOCK_INTEGER_MULTI[1]").read(json));
        assertEquals(3,				JsonPath.compile("INFO.MOCK_INTEGER_MULTI[2]").read(json));
        assertEquals("fake_id",		JsonPath.compile(CoreAttributes._id.toString()).read(json));
        assertEquals("20",			JsonPath.compile(CoreAttributes._landmark.toString()).read(json));
        assertEquals(9076,			JsonPath.compile(CoreAttributes._minBP.toString()).read(json));
        assertEquals(9080,			JsonPath.compile(CoreAttributes._maxBP.toString()).read(json));
        assertEquals("AGAAA",		JsonPath.compile(CoreAttributes._refAllele.toString()).read(json));
        assertEquals("A",			JsonPath.compile(CoreAttributes._altAlleles.toString()+"[0]").read(json));
        assertEquals(Type.VARIANT.toString(),	JsonPath.compile(CoreAttributes._type.toString()).read(json));
        
        // grab 3nd row of data only
        pipeline.hasNext();	    
        history = pipeline.next();
        json = history.get(history.size() - 1);
        
        // test for a field that shows up in INFO but is NOT defined in the header
        assertEquals("123",				JsonPath.compile("INFO.UNKNOWN_FIELD").read(json));
        
        // grab 4th row of data only, check to see if multiple alleles gets in correctly
        pipeline.hasNext();	    
        history = pipeline.next();
        json = history.get(history.size() - 1);
        assertEquals("A",			JsonPath.compile(CoreAttributes._altAlleles.toString()+"[0]").read(json));
        assertEquals("T",			JsonPath.compile(CoreAttributes._altAlleles.toString()+"[1]").read(json));
        
        // grab 5th row of data only, check to see if "." period is handled correctly for
        // INTEGER and FLOAT column types
        pipeline.hasNext();	    
        history = pipeline.next();
        json = history.get(history.size() - 1);
        
        // use JSON paths to drill out values and compare with expected
        assertFalse(json.contains("INFO.MOCK_INTEGER"));
        assertFalse(json.contains("INFO.MOCK_FLOAT"));
        assertEquals(99,		JsonPath.compile("INFO.MOCK_INTEGER_MULTI[0]").read(json));
        assertEquals(333,		JsonPath.compile("INFO.MOCK_INTEGER_MULTI[1]").read(json));
        assertEquals(11.11,		JsonPath.compile("INFO.MOCK_FLOAT_MULTI[0]").read(json));
        assertEquals(777.77,	JsonPath.compile("INFO.MOCK_FLOAT_MULTI[1]").read(json));

        assertFalse(json.contains("INFO.MOCK_INTEGER_MULTI"));
    }    
}
