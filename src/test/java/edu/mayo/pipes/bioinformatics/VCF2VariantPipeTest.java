/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.gson.JsonObject;
import edu.mayo.pipes.UNIX.GrepEPipe;
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

import static org.junit.Assert.*;

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

    @Test
    public void testDoubleDot(){
        //sometimes users give VCF files with a number that can not be parsed, for example ..2 instead of 0.2
        //while we may be able to correct these problems on a case by case basis, we don't know what the correction is
        // for any case we may get.  This tests, that if the the parse fails - the attribute is just not added to the json and
        // the parser continues processing.
        Pipeline p = new Pipeline(
                new CatPipe(),
                new HistoryInPipe(),
                new VCF2VariantPipe(),
                new HistoryOutPipe(),
                new GrepEPipe("#")
        );
        String line1 = "chr1\t914964\t.\tC\tA\t8.46\tLowQual\tAC=2;AF=0.25;AN=8;BaseQRankSum=-1.904;DP=174;Dels=0.00;FS=0.000;HRun=0;HaplotypeScore=0.2080;MQ=146.61;MQ0=3;MQRankSum=..501;QD=4.23;ReadPosRankSum=-1.286;SB=-37.62;SNPEFF_EFFECT=DOWNSTREAM;SNPEFF_FUNCTIONAL_CLASS=NONE;SNPEFF_GENE_NAME=PLEKHN1;SNPEFF_IMPACT=MODIFIER;SNPEFF_TRANSCRIPT_ID=NM_001160184;set=FilteredInAll;CSQ=NA|NA\tGT:AD:DP:GQ:PL\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t0/0:170,0:170:99:0,451,6234\t0/0:1,0:1:3.01:0,3,42\t1/1:0,2:2:3.01:45,3,0\t0/0:1,0:1:3.01:0,3,41\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t0\t{\"CHROM\":\"chr1\",\"POS\":\"914964\",\"ID\":\".\",\"REF\":\"C\",\"ALT\":\"A\",\"QUAL\":\"8.46\",\"FILTER\":\"LowQual\",\"INFO\":{\"AC\":[2],\"AF\":[0.25],\"AN\":8,\"BaseQRankSum\":-1.904,\"DP\":174,\"Dels\":0.0,\"FS\":0.0,\"HRun\":0,\"HaplotypeScore\":0.208,\"MQ\":146.61,\"MQ0\":3,\"QD\":4.23,\"ReadPosRankSum\":-1.286,\"SB\":-37.62,\"SNPEFF_EFFECT\":\"DOWNSTREAM\",\"SNPEFF_FUNCTIONAL_CLASS\":\"NONE\",\"SNPEFF_GENE_NAME\":\"PLEKHN1\",\"SNPEFF_IMPACT\":\"MODIFIER\",\"SNPEFF_TRANSCRIPT_ID\":\"NM_001160184\",\"set\":\"FilteredInAll\",\"CSQ\":[\"NA|NA\"]},\"_id\":\".\",\"_type\":\"variant\",\"_landmark\":\"1\",\"_refAllele\":\"C\",\"_altAlleles\":[\"A\"],\"_minBP\":914964,\"_maxBP\":914964}";
        String line2 = "chr1\t1291078\t.\tC\tG\t10.45\tLowQual\tAC=2;AF=0.50;AN=4;BaseQRankSum=0.480;DP=10;Dels=0.00;FS=0.000;HRun=1;HaplotypeScore=0.3943;MQ=135.49;MQ0=0;MQRankSum=-1.271;QD=5.23;ReadPosRankSum=0.000;SB=-38.36;SNPEFF_AMINO_ACID_CHANGE=S43T;SNPEFF_AMINO_ACID_LENGTH=442;SNPEFF_CODON_CHANGE=aGc/aCc;SNPEFF_EFFECT=NON_SYNONYMOUS_CODING;SNPEFF_EXON_ID=NM_032348.ex.8;SNPEFF_FUNCTIONAL_CLASS=MISSENSE;SNPEFF_GENE_NAME=MXRA8;SNPEFF_IMPACT=MODERATE;SNPEFF_TRANSCRIPT_ID=NM_032348;set=FilteredInAll;CSQ=benign|tolerated\tGT:AD:DP:GQ:PL\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t0/0:8,0:8:21.07:0,21,290\t./.\t./.\t1/1:0,2:2:3.01:45,3,0\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t./.\t0\t{\"CHROM\":\"chr1\",\"POS\":\"1291078\",\"ID\":\".\",\"REF\":\"C\",\"ALT\":\"G\",\"QUAL\":\"10.45\",\"FILTER\":\"LowQual\",\"INFO\":{\"AC\":[2],\"AF\":[0.5],\"AN\":4,\"BaseQRankSum\":0.48,\"DP\":10,\"Dels\":0.0,\"FS\":0.0,\"HRun\":1,\"HaplotypeScore\":0.3943,\"MQ\":135.49,\"MQ0\":0,\"MQRankSum\":-1.271,\"QD\":5.23,\"ReadPosRankSum\":0.0,\"SB\":-38.36,\"SNPEFF_AMINO_ACID_CHANGE\":\"S43T\",\"SNPEFF_AMINO_ACID_LENGTH\":\"442\",\"SNPEFF_CODON_CHANGE\":\"aGc/aCc\",\"SNPEFF_EFFECT\":\"NON_SYNONYMOUS_CODING\",\"SNPEFF_EXON_ID\":\"NM_032348.ex.8\",\"SNPEFF_FUNCTIONAL_CLASS\":\"MISSENSE\",\"SNPEFF_GENE_NAME\":\"MXRA8\",\"SNPEFF_IMPACT\":\"MODERATE\",\"SNPEFF_TRANSCRIPT_ID\":\"NM_032348\",\"set\":\"FilteredInAll\",\"CSQ\":[\"benign|tolerated\"]},\"_id\":\".\",\"_type\":\"variant\",\"_landmark\":\"1\",\"_refAllele\":\"C\",\"_altAlleles\":[\"G\"],\"_minBP\":1291078,\"_maxBP\":1291078}";
        p.setStarts(Arrays.asList("src/test/resources/testData/badMQRankSum.vcf"));
        for(int i=1; p.hasNext(); i++){
            String s = (String) p.next();
            if(i==1){
                assertEquals(line1, s);
            }
            if(i==2){
                assertEquals(line2, s);
            }
            //System.out.println(s);
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
        System.out.println(json);
        
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
        VCF2VariantPipe vcf 	= new VCF2VariantPipe(true, false);
        String drill1 = "samples.[17].GenotypePositive";//.GenotypePositive
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


        String json = "{\"CHROM\":\"chr1\",\"POS\":\"28218100\",\"ID\":\".\",\"REF\":\"T\",\"ALT\":\"C\",\"QUAL\":\"161.75\",\"FILTER\":\".\",\"INFO\":{\"AC\":[39],\"AF\":[0.342],\"AN\":114,\"BaseQRankSum\":-2.185,\"DP\":22,\"Dels\":0.0,\"FS\":4.193,\"HaplotypeScore\":0.0,\"MLEAC\":[37],\"MLEAF\":[0.325],\"MQ\":70.0,\"MQ0\":0,\"MQRankSum\":-0.282,\"QD\":20.22,\"ReadPosRankSum\":-1.128},\"_id\":\".\",\"_type\":\"variant\",\"_landmark\":\"1\",\"_refAllele\":\"T\",\"_altAlleles\":[\"C\"],\"_minBP\":28218100,\"_maxBP\":28218100,\"samples\":[{\"GT\":\"0/0/0/0/0/0\",\"AD\":[1.0,0.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":0.0,\"MLPSAF\":0.0,\"PL\":[0.0,1.0,2.0,3.0,5.0,8.0,33.0],\"sampleID\":\"s_Mayo_TN_CC_319\"},{\"GT\":\"0/0/0/0/0/0\",\"AD\":[1.0,0.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":0.0,\"MLPSAF\":0.0,\"PL\":[0.0,1.0,2.0,3.0,5.0,8.0,39.0],\"sampleID\":\"s_Mayo_TN_CC_322\"},{\"GT\":\"1/1/1/1/1/1\",\"AD\":[0.0,1.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":6.0,\"MLPSAF\":1.0,\"PL\":[34.0,8.0,5.0,3.0,2.0,1.0,0.0],\"GenotypePositive\":1,\"sampleID\":\"s_Mayo_TN_CC_327\"},{\"GT\":\"0/0/0/1/1/1\",\"AD\":[1.0,1.0],\"DP\":2.0,\"GQ\":1.0,\"MLPSAC\":3.0,\"MLPSAF\":0.5,\"PL\":[30.0,3.0,1.0,0.0,1.0,3.0,30.0],\"GenotypePositive\":1,\"sampleID\":\"s_Mayo_TN_CC_335\"},{\"GT\":\"0/0/0/0/0/0\",\"AD\":[1.0,0.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":0.0,\"MLPSAF\":0.0,\"PL\":[0.0,1.0,2.0,3.0,5.0,8.0,37.0],\"sampleID\":\"s_Mayo_TN_CC_337\"},{\"GT\":\"0/0/0/0/0/0\",\"AD\":[1.0,0.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":0.0,\"MLPSAF\":0.0,\"PL\":[0.0,1.0,2.0,3.0,5.0,8.0,37.0],\"sampleID\":\"s_Mayo_TN_CC_339\"},{\"GT\":\"1/1/1/1/1/1\",\"AD\":[0.0,1.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":6.0,\"MLPSAF\":1.0,\"PL\":[37.0,8.0,5.0,3.0,2.0,1.0,0.0],\"GenotypePositive\":1,\"sampleID\":\"s_Mayo_TN_CC_348\"},{\"GT\":\"0/0/0/0/0/0\",\"AD\":[1.0,0.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":0.0,\"MLPSAF\":0.0,\"PL\":[0.0,1.0,2.0,3.0,5.0,8.0,38.0],\"sampleID\":\"s_Mayo_TN_CC_350\"},{\"GT\":\"1/1/1/1/1/1\",\"AD\":[0.0,1.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":6.0,\"MLPSAF\":1.0,\"PL\":[32.0,8.0,5.0,3.0,2.0,1.0,0.0],\"GenotypePositive\":1,\"sampleID\":\"s_Mayo_TN_CC_361\"},{\"GT\":\"1/1/1/1/1/1\",\"AD\":[0.0,1.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":6.0,\"MLPSAF\":1.0,\"PL\":[26.0,8.0,5.0,3.0,2.0,1.0,0.0],\"GenotypePositive\":1,\"sampleID\":\"s_Mayo_TN_CC_364\"},{\"GT\":\"0/0/0/0/0/0\",\"AD\":[1.0,0.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":0.0,\"MLPSAF\":0.0,\"PL\":[0.0,1.0,2.0,3.0,5.0,8.0,36.0],\"sampleID\":\"s_Mayo_TN_CC_367\"},{\"GT\":\"1/1/1/1/1/1\",\"AD\":[0.0,1.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":6.0,\"MLPSAF\":1.0,\"PL\":[30.0,8.0,5.0,3.0,2.0,1.0,0.0],\"GenotypePositive\":1,\"sampleID\":\"s_Mayo_TN_CC_374\"},{\"GT\":\"0/0/0/0/0/0\",\"AD\":[1.0,0.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":0.0,\"MLPSAF\":0.0,\"PL\":[0.0,1.0,2.0,3.0,5.0,8.0,37.0],\"sampleID\":\"s_Mayo_TN_CC_377\"},{\"GT\":\"0/0/0/0/0/0\",\"AD\":[1.0,0.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":0.0,\"MLPSAF\":0.0,\"PL\":[0.0,1.0,2.0,3.0,5.0,8.0,38.0],\"sampleID\":\"s_Mayo_TN_CC_380\"},{\"GT\":\"1/1/1/1/1/1\",\"AD\":[0.0,1.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":6.0,\"MLPSAF\":1.0,\"PL\":[37.0,8.0,5.0,3.0,2.0,1.0,0.0],\"GenotypePositive\":1,\"sampleID\":\"s_Mayo_TN_CC_382\"},{\"GT\":\"0/0/0/0/0/0\",\"AD\":[1.0,0.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":0.0,\"MLPSAF\":0.0,\"PL\":[0.0,1.0,2.0,3.0,5.0,8.0,36.0],\"sampleID\":\"s_Mayo_TN_CC_384\"},{\"GT\":\"0/0/0/0/0/0\",\"AD\":[1.0,0.0],\"DP\":1.0,\"GQ\":1.0,\"MLPSAC\":0.0,\"MLPSAF\":0.0,\"PL\":[0.0,1.0,2.0,3.0,5.0,8.0,36.0],\"sampleID\":\"s_Mayo_TN_CC_389\"},{\"GT\":\"0/0/0/0/0/0\",\"AD\":[2.0,0.0],\"DP\":2.0,\"GQ\":2.0,\"MLPSAC\":0.0,\"MLPSAF\":0.0,\"PL\":[0.0,2.0,4.0,6.0,10.0,16.0,77.0],\"sampleID\":\"s_Mayo_TN_CC_394\"},{\"GT\":\"0/0/0/0/0/0\",\"AD\":[2.0,0.0],\"DP\":2.0,\"GQ\":2.0,\"MLPSAC\":0.0,\"MLPSAF\":0.0,\"PL\":[0.0,2.0,4.0,6.0,10.0,16.0,64.0],\"sampleID\":\"s_Mayo_TN_CC_398\"}],\"FORMAT\":{\"max\":{\"PL\":77.0,\"AD\":2.0,\"GQ\":2.0,\"DP\":2.0,\"MLPSAF\":1.0,\"MLPSAC\":6.0},\"min\":{\"PL\":0.0,\"AD\":0.0,\"GQ\":1.0,\"DP\":1.0,\"MLPSAF\":0.0,\"MLPSAC\":0.0},\"GenotypePostitiveCount\":7,\"GenotypePositiveList\":[\"s_Mayo_TN_CC_327\",\"s_Mayo_TN_CC_335\",\"s_Mayo_TN_CC_348\",\"s_Mayo_TN_CC_361\",\"s_Mayo_TN_CC_364\",\"s_Mayo_TN_CC_374\",\"s_Mayo_TN_CC_382\"]}}";
        for(int i=0; pipeline.hasNext(); i++){
            String next = pipeline.next();
            System.out.println(next);

            if(i==0){
                assertEquals(json, next);
            }

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
    public void testBuildFormatJSON(){
        String vcfLine = "chr1\t756258\tBND_qdnezqbk\tT\t]chr1:756327]T\t2.38\tPASS\tSVTYPE=BND;EVENT=NOV_INS;ISIZE=69;MATE_ID=BND_whmomhxb\tGT:CTX:DEL:INS:INV:NOV_INS:TDUP:lSC:nSC:uRP:distl_levD\t0/1:0:0:1:0:15:2:52:9:15:0.42\t0/0:.:.:.:.:.:.:.:.:.:.\t0/0:.:.:.:.:.:.:.:.:.:.\n";
        List<String> line = Arrays.asList(vcfLine.split("\t"));
        VCF2VariantPipe vcf = new VCF2VariantPipe(true);
        JsonObject ob = vcf.buildFormatJSON(line);
        assertNotNull(ob);
        String formatjson = "{\"max\":{\"lSC\":52.0,\"INS\":1.0,\"nSC\":9.0,\"NOV_INS\":15.0,\"CTX\":0.0,\"uRP\":15.0,\"TDUP\":2.0,\"INV\":0.0,\"DEL\":0.0,\"distl_levD\":0.42},\"min\":{\"lSC\":52.0,\"INS\":1.0,\"nSC\":9.0,\"NOV_INS\":15.0,\"CTX\":0.0,\"uRP\":15.0,\"TDUP\":2.0,\"INV\":0.0,\"DEL\":0.0,\"distl_levD\":0.42}}";
        assertEquals(formatjson, ob.toString());
        //System.out.println(ob.toString());
    }
    
    @Test
    public void sampleHasVariant(){
        VCF2VariantPipe vcf 	= new VCF2VariantPipe(true);
        assertEquals(false, vcf.sampleHasVariant("./././././."));
        assertEquals(false, vcf.sampleHasVariant("0/0/0/0/0/0"));
        assertEquals(false, vcf.sampleHasVariant("0|0|0|0|0|0"));
        assertEquals(true, vcf.sampleHasVariant("1/1/1/1/1/1"));
    }


    @Test
    public void testGetEntryType(){
        VCF2VariantPipe vcf = new VCF2VariantPipe(true);
        assertEquals("source",vcf.getEntryType("##source=myImputationProgramV3.1"));
        assertEquals("INFO",vcf.getEntryType("##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Total Depth\">"));
        assertEquals("FILTER",vcf.getEntryType("##FILTER=<ID=s50,Description=\"Less than 50% of samples have data\">"));
        assertEquals("FORMAT",vcf.getEntryType("##FORMAT=<ID=HQ,Number=2,Type=Integer,Description=\"Haplotype Quality\">"));
    }
    
}
