/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import com.tinkerpop.pipes.transform.IdentityPipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.JSON.SimpleDrillPipe;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.ReplaceAllPipe;

import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.bioinformatics.VCF2VariantPipe.InfoFieldMeta;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.bioinformatics.vocab.Type;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.history.HistoryOutPipe;
import java.util.Map;

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
     * Tests for malformed VCF file.
     */
    @Test
    public void testBadVCF() {
    	// VCF file where required TAB delimiter is a whitespace char instead
    	List<String> vcfLinesNoTabs = new ArrayList<String>();
		vcfLinesNoTabs.add("##fileformat=VCFv4.0\n");
		vcfLinesNoTabs.add("#CHROM POS ID REF ALT QUAL FILTER INFO\n");
		vcfLinesNoTabs.add("2 48010558 rs1042820 C A . . \n");    	
    	checkBadVCF(vcfLinesNoTabs);
    	
    	// VCF file with only 1 column
    	List<String> vcfLinesOneColumn = new ArrayList<String>();
		vcfLinesOneColumn.add("##fileformat=VCFv4.0\n");
		vcfLinesOneColumn.add("#CHROM\n");
		vcfLinesOneColumn.add("2\n");    	
    	checkBadVCF(vcfLinesOneColumn);    	
    }
    
    /**
     * Helper method to process a malformed VCF and checks for expected exception.
     */
    private void checkBadVCF(List<String> vcfLines) {		
    	// pipes
    	HistoryInPipe historyIn = new HistoryInPipe();
        VCF2VariantPipe vcf 	= new VCF2VariantPipe();
        
        Pipe<String, History> pipeline = new Pipeline<String, History>
        	(
        		historyIn,	// String			--> history
        		vcf			// history			--> add JSON to end of history
        	);
        pipeline.setStarts(vcfLines);

        // attempt to grab 1st row of data
        try {
            assertTrue(pipeline.hasNext());        	
            pipeline.next();
            
            // expected an Exception
            fail("");
        } catch (RuntimeException re) {
        	//expected
        }
    }
    
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
    
    
    
    /**
     *  VCF2VariantPipe - test to make sure the sample processing works
     */
    @Test
    public void testSamples() {
    	
    	// pipes
    	CatPipe			cat 	= new CatPipe();
    	HistoryInPipe historyIn = new HistoryInPipe();
        VCF2VariantPipe vcf 	= new VCF2VariantPipe(true);
        String drill1 = "samples.[87].GenotypePositive";//.GenotypePositive
        JsonPath jsonPath1 = JsonPath.compile(drill1);
        
        Pipe<String, String> pipeline = new Pipeline<String, String>
        	(
        		cat,		// read VCF line	--> String
        		historyIn,	// String			--> history
        		vcf,			// history			--> add JSON to end of history
                        new MergePipe("\t"),
                        new ReplaceAllPipe("^.*\t\\{", "{"),
                        //new PrintPipe()
                        new IdentityPipe()
        	);
        pipeline.setStarts(Arrays.asList("src/test/resources/testData/VCF/BATCH4_first2000.vcf"));

        for(int i=0; pipeline.hasNext(); i++){
            String next = pipeline.next();
            //
            if(i==1){            
                Object o = jsonPath1.read(next);
                System.out.println(o.toString());
                if(!o.equals(null)){
                    assertEquals("1",o.toString());
                }else {
                    assertEquals(false, true);//data did not have desired result!
                }
                break;
            }
        }

        /*
        TODO: this should output something, still need to test it:
        {"INFO":{"HaplotypeScore":{"number":1,"type":"Float"},"InbreedingCoeff":{"number":1,"type":"Float"},"MLEAC":{"number":null,"type":"Integer"},"MLEAF":{"number":null,"type":"Float"},"FS":{"number":1,"type":"Float"},"ReadPosRankSum":{"number":1,"type":"Float"},"DP":{"number":1,"type":"Integer"},"DS":{"number":0,"type":"Flag"},"STR":{"number":0,"type":"Flag"},"BaseQRankSum":{"number":1,"type":"Float"},"QD":{"number":1,"type":"Float"},"MQ":{"number":1,"type":"Float"},"AC":{"number":null,"type":"Integer"},"PL":{"number":null,"type":"Integer"},"AD":{"number":null,"type":"Integer"},"GT":{"number":1,"type":"String"},"MQRankSum":{"number":1,"type":"Float"},"RU":{"number":1,"type":"String"},"Dels":{"number":1,"type":"Float"},"GQ":{"number":1,"type":"Integer"},"RPA":{"number":null,"type":"Integer"},"AF":{"number":null,"type":"Float"},"MLPSAF":{"number":null,"type":"Float"},"MQ0":{"number":1,"type":"Integer"},"MLPSAC":{"number":null,"type":"Integer"},"AN":{"number":1,"type":"Integer"}},"FORMAT":{"PL":1,"AD":1,"GT":1,"GQ":1,"DP":1,"MLPSAF":1,"MLPSAC":1},"SAMPLES":{"s_Mayo_TN_CC_394":91,"s_Mayo_TN_CC_393":90,"s_Mayo_TN_CC_392":89,"s_Mayo_TN_CC_391":88,"s_Mayo_TN_CC_398":95,"s_Mayo_TN_CC_397":94,"s_Mayo_TN_CC_396":93,"s_Mayo_TN_CC_395":92,"s_Mayo_TN_CC_350":47,"s_Mayo_TN_CC_390":87,"s_Mayo_TN_CC_353":50,"s_Mayo_TN_CC_354":51,"s_Mayo_TN_CC_351":48,"s_Mayo_TN_CC_352":49,"s_Mayo_TN_CC_358":55,"s_Mayo_TN_CC_357":54,"s_Mayo_TN_CC_356":53,"s_Mayo_TN_CC_355":52,"s_Mayo_TN_CC_359":56,"s_Mayo_TN_CC_399":96,"s_Mayo_TN_CC_381":78,"s_Mayo_TN_CC_380":77,"s_Mayo_TN_CC_383":80,"s_Mayo_TN_CC_382":79,"s_Mayo_TN_CC_385":82,"s_Mayo_TN_CC_319":16,"s_Mayo_TN_CC_384":81,"s_Mayo_TN_CC_387":84,"s_Mayo_TN_CC_386":83,"s_Mayo_TN_CC_315":12,"s_Mayo_TN_CC_316":13,"s_Mayo_TN_CC_317":14,"s_Mayo_TN_CC_318":15,"s_Mayo_TN_CC_340":37,"s_Mayo_TN_CC_341":38,"s_Mayo_TN_CC_313":10,"s_Mayo_TN_CC_342":39,"s_Mayo_TN_CC_314":11,"s_Mayo_TN_CC_343":40,"s_Mayo_TN_CC_345":42,"s_Mayo_TN_CC_344":41,"s_Mayo_TN_CC_347":44,"s_Mayo_TN_CC_346":43,"s_Mayo_TN_CC_349":46,"s_Mayo_TN_CC_348":45,"s_Mayo_TN_CC_388":85,"s_Mayo_TN_CC_389":86,"s_Mayo_TN_CC_375":72,"s_Mayo_TN_CC_324":21,"s_Mayo_TN_CC_376":73,"s_Mayo_TN_CC_325":22,"s_Mayo_TN_CC_373":70,"s_Mayo_TN_CC_322":19,"s_Mayo_TN_CC_374":71,"s_Mayo_TN_CC_323":20,"s_Mayo_TN_CC_371":68,"s_Mayo_TN_CC_328":25,"s_Mayo_TN_CC_372":69,"s_Mayo_TN_CC_329":26,"s_Mayo_TN_CC_326":23,"s_Mayo_TN_CC_370":67,"s_Mayo_TN_CC_327":24,"s_Mayo_TN_CC_321":18,"s_Mayo_TN_CC_379":76,"s_Mayo_TN_CC_320":17,"s_Mayo_TN_CC_378":75,"s_Mayo_TN_CC_377":74,"s_Mayo_TN_CC_362":59,"s_Mayo_TN_CC_333":30,"s_Mayo_TN_CC_363":60,"s_Mayo_TN_CC_334":31,"s_Mayo_TN_CC_364":61,"s_Mayo_TN_CC_335":32,"s_Mayo_TN_CC_365":62,"s_Mayo_TN_CC_336":33,"s_Mayo_TN_CC_337":34,"s_Mayo_TN_CC_338":35,"s_Mayo_TN_CC_339":36,"s_Mayo_TN_CC_360":57,"s_Mayo_TN_CC_361":58,"s_Mayo_TN_CC_407":104,"s_Mayo_TN_CC_367":64,"s_Mayo_TN_CC_330":27,"s_Mayo_TN_CC_408":105,"s_Mayo_TN_CC_366":63,"s_Mayo_TN_CC_369":66,"s_Mayo_TN_CC_332":29,"s_Mayo_TN_CC_368":65,"s_Mayo_TN_CC_331":28,"s_Mayo_TN_CC_403":100,"s_Mayo_TN_CC_404":101,"s_Mayo_TN_CC_405":102,"s_Mayo_TN_CC_406":103,"s_Mayo_TN_CC_400":97,"s_Mayo_TN_CC_401":98,"s_Mayo_TN_CC_402":99}}
        */ 
        System.out.println(vcf.getJSONMetadata());

    }  
    
    
    @Test
    public void sampleHasVariant(){
        VCF2VariantPipe vcf 	= new VCF2VariantPipe(true);
        assertEquals(false, vcf.sampleHasVariant("./././././."));
        assertEquals(false, vcf.sampleHasVariant("0/0/0/0/0/0"));
        assertEquals(false, vcf.sampleHasVariant("0|0|0|0|0|0"));
        assertEquals(true, vcf.sampleHasVariant("1/1/1/1/1/1"));
    }
    
    
}
