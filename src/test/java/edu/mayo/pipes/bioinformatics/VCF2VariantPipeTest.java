/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics;
import static org.junit.Assert.assertEquals;

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

import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.bioinformatics.vocab.Type;

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

    /**
     * Test of processNextStart method, of class VCF2VariantPipe.
     */
    @Test
    public void testProcessNextStart() {
    	// pipes
    	CatPipe			cat 	= new CatPipe();
    	SplitPipe		split	= new SplitPipe("\t");
        VCF2VariantPipe vcf 	= new VCF2VariantPipe();
        
        Pipe<String, List<String>> pipeline = new Pipeline<String, List<String>>
        	(
        		cat,	// read VCF line	--> String
        		split,	// split String 	-->	List<String> history
        		vcf		// history			--> add JSON to end of history
        	);
        pipeline.setStarts(Arrays.asList("src/test/resources/testData/vcf-format-4_0.vcf"));

        // grab 1st row of data
        pipeline.hasNext();	    
        List<String> history = pipeline.next();
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
    }
}
