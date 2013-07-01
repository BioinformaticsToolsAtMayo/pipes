/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
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

import edu.mayo.pipes.JSON.DrillPipe;
import edu.mayo.pipes.bioinformatics.VCF2VariantPipe;
import edu.mayo.pipes.exceptions.InvalidPipeInputException;
import edu.mayo.pipes.history.HCutPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.util.test.PipeTestUtils;

/**
 *
 */
public class SameVariantPipeTest {
    
    public SameVariantPipeTest() {
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
    
    private String catalogFile = "src/test/resources/testData/sameVariantCatalog.tsv.gz";

    
    /**
     * POSSIBLE TEST CASES
     * 1. Variant with no match.
     * 2. Variant with 1 match.
     * 3. Variant with more than 1 matches.
     * 4. Variant with Position that is same, but not matched
     * 5. Variant with Chrom + Pos that is same, but not matched
     * 6. Variant that matched on rsId + Chr + Pos, but if Flag is ON, then it shouldn't match
     * 7. Variant that matched on ref/alt + Chr + Pos, but if Flag is ON, then it shouldn't match
     * 8. If both Flags are ON.. throw and exception
     * 9. Test for ALT:
     * 		in the file: C, T
     * 		input: A
     *      Output options: 
     *      	A -> C : OK; 
     *      	A -> T : OK; 
     *      	A -> [C,T] : OK
     *      	A -> any other than C, T.. FAIL
     * 
     */
    
    /**
     * 1. Variant with no match.
     * @throws IOException
     */
    @Test
    public void testNoMatch() throws IOException{
        String variantThatDoesNotMatch = "20	24970000	rsFOOBAZ	T	C	.	.	.";
        Pipe same = new SameVariantPipe(catalogFile);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantThatDoesNotMatch));
        History next = (History) p.next();
        assertEquals("{}", next.get(9));
    }

    /**
     * 2. Variant with 1 match.
     * @throws IOException
     */
    @Test
    public void testOneMatch() throws IOException{
        String variantOneMatch = "21	26960070	rs116645811	G	A	.	.	.";
        Pipe same = new SameVariantPipe(catalogFile);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantOneMatch));
        
        int resultCount = 0;
        while(p.hasNext()) {
        	History next = (History) p.next();
        	resultCount++;
            //System.out.println(next.get(9));
            assertEquals("21", JsonPath.compile("CHROM").read(next.get(9)));
            assertEquals("26960070", JsonPath.compile("POS").read(next.get(9)));
            assertEquals("rs116645811", JsonPath.compile("ID").read(next.get(9)));
        }
        assertEquals(1, resultCount); //only one row need to be returned
    }

    /******************************************************************************************
     * 3. Variant with multiple matches.
     * @throws IOException
     */
    @Test
    public void testMultipleMatch() throws IOException{
        String variantMultipleMatch = "21	26976144	rs116331755	A	G	.	.	.";
        Pipe same = new SameVariantPipe(catalogFile);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantMultipleMatch));

        int resultCount = 0;
        while(p.hasNext()){
            History next = (History) p.next();
        	resultCount++;
        	
        	// This loops goes thru 3 times, in each case, Chrom, Pos and ID must be the same. So this assert should not fail.
            assertEquals("21", JsonPath.compile("CHROM").read(next.get(9)));
            assertEquals("26976144", JsonPath.compile("POS").read(next.get(9)));
            assertEquals("rs116331755", JsonPath.compile("ID").read(next.get(9)));
        }
        assertEquals(3, resultCount); //only one row need to be returned
    }
    
    /***********************************************************************************
     * 4. Variant with position that is same, but not matched
     * 4a) Same Pos; Different Chr, Different rsId, same Ref, same Alt - return the match
     * 4b) Same Pos; Different Chr, Different rsId, different Ref, same Alt - empty
     * 4c) Same Pos; Different Chr, Different rsId, different Ref, different Alt - empty
     */
    @Test
    public void testVariantSamePositionNoMatch1() throws IOException{
        String variantSamePositionNoMatch = "22	26960070	BADRSID	G	A	.	.	.";
        Pipe same = new SameVariantPipe(catalogFile);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));
        History next = (History) p.next();
        assertEquals("{}", next.get(9)); // EMPTY
    }

    /**
     * 4b) Same Chr+Pos; Different rsId, different Ref, same Alt - empty
     */
    @Test
    public void testVariantSamePositionNoMatch2() throws IOException{
        String variantSamePositionNoMatch = "22	26960070	BADRSID	X	A	.	.	.";
        Pipe same = new SameVariantPipe(catalogFile);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));
        History next = (History) p.next();
        assertEquals("{}", next.get(9)); // EMPTY
    }

    /**
     * 4c) Same Chr+Pos; Different rsId, different Ref, different Alt - empty
     */
    @Test
    public void testVariantSamePositionNoMatch3() throws IOException{
        String variantSamePositionNoMatch = "22	26960070	BADRSID	X	Y	.	.	.";
        Pipe same = new SameVariantPipe(catalogFile);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));
        History next = (History) p.next();
        assertEquals("{}", next.get(9)); // EMPTY
    }

    /******************************************************************************8
     * 5. Variant with Chr+Position that is same, but not matched
     * 5a) Same Chr+Pos; Different rsId, same Ref, same Alt - return the match
     * 5b) Same Chr+Pos; Different rsId, different Ref, same Alt - empty
     * 5c) Same Chr+Pos; Different rsId, different Ref, different Alt - empty
     */
    @Test
    public void testVariantSameChrPositionNoMatch1() throws IOException{
        String variantSamePositionNoMatch = "21	26960070	BADRSID	G	A	.	.	.";
        Pipe same = new SameVariantPipe(catalogFile);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));

        int resultCount = 0;
        while(p.hasNext()){
            History next = (History) p.next();
        	resultCount++;
        	
        	// This loops goes thru 3 times, in each case, Chrom, Pos and ID must be the same. So this assert should not fail.
            assertEquals("21", JsonPath.compile("CHROM").read(next.get(9)));
            assertEquals("26960070", JsonPath.compile("POS").read(next.get(9)));
            assertEquals("G", JsonPath.compile("REF").read(next.get(9)));
            assertEquals("A", JsonPath.compile("ALT").read(next.get(9)));
        }
        assertEquals(1, resultCount); //only one row need to be returned
    }

    /**
     * 5b) Same Chr+Pos; Different rsId, different Ref, same Alt - empty
     */
    @Test
    public void testVariantSameChrPositionNoMatch2() throws IOException{
        String variantSamePositionNoMatch = "21	26960070	BADRSID	X	A	.	.	.";
        Pipe same = new SameVariantPipe(catalogFile);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));
        History next = (History) p.next();
        assertEquals("{}", next.get(9)); // EMPTY
    }

    /**
     * 5c) Same Chr+Pos; Different rsId, different Ref, different Alt - empty
     */
    @Test
    public void testVariantSameChrPositionNoMatch3() throws IOException{
        String variantSamePositionNoMatch = "21	26960070	BADRSID	X	Y	.	.	.";
        Pipe same = new SameVariantPipe(catalogFile);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));
        History next = (History) p.next();
        assertEquals("{}", next.get(9)); // EMPTY
    }

    /*************************************************************
     * * 6. Variant that matched on rsId + Chr + Pos, but if rsIDFlag is ON, then it shouldn't match
     */
    @Test
    public void testRsIdFlagTrue1() throws IOException{
    	// RSID: Matched and rsIDFlag=True.. then return row or check based only on RSID
    	String variantSamePositionNoMatch = "21\t26960070\trs116645811\tG\tA\t.\t.\t.";
        Pipe same = new SameVariantPipe(catalogFile, true, false, -1);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));

        History next = (History) p.next();
       	assertEquals("rs116645811", JsonPath.compile("ID").read(next.get(9)));        	
    }

    /**
     * * 6b. 
     */
    @Test
    public void testRsIdFlagTrue2() throws IOException{
    	// RSID: NOT MATCHED and rsIDFlag=FLASE.. then return EMPTY
    	String variantSamePositionNoMatch = "21\t26960070\tBADRSID\tG\tA\t.\t.\t.";
        Pipe same = new SameVariantPipe(catalogFile, true, false, -1);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));

        History next = (History) p.next();
       	assertEquals("{}", next.get(9));        	
    }
    
    /**
     * * 6c. 
     */
    @Test
    public void testRsIdFlagFlase1() throws IOException{
    	// RSID: MATCHED and rsIDFlag=FALSE.. then return ROWif found baed on chr, pos
    	String variantSamePositionNoMatch = "21\t26960070\trs1166458118\tG\tA\t.\t.\t.";
        Pipe same = new SameVariantPipe(catalogFile, false, false, -1);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));

        History next = (History) p.next();
        assertEquals("21", JsonPath.compile("CHROM").read(next.get(9)));
        assertEquals("26960070", JsonPath.compile("POS").read(next.get(9)));
       	assertEquals("rs116645811", JsonPath.compile("ID").read(next.get(9)));        	
    }
    

    /**
     * * 6d. 
     */
    @Test
    public void testRsIdFlagFlase2() throws IOException{
    	// RSID: NOT MATCHED and rsIDFlag=FALSE.. then return ROWif found baed on chr, pos
    	String variantSamePositionNoMatch = "21\t26960070\tBADRSID\tG\tA\t.\t.\t.";
        Pipe same = new SameVariantPipe(catalogFile, false, false, -1);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));

        History next = (History) p.next();
        assertEquals("21", JsonPath.compile("CHROM").read(next.get(9)));
        assertEquals("26960070", JsonPath.compile("POS").read(next.get(9)));
        assertEquals("rs116645811", JsonPath.compile("ID").read(next.get(9)));       	        	
    }
    
    /****************************************************
     * 7. Variant that matched on ref/alt + Chr + Pos, but if AlleleFlag is ON, then it shouldn't match
     * 
     * 7a: Ref: Matched, Alt: is a subset and AlleleFlag=True.. then return row or check based only on RSID
     */
    @Test 
    public void testAlleleFlagTrue1() throws IOException{
    	// Ref: Matched, Alt: is a subset and AlleleFlag=True.. then return row or check based only on RSID
    	String variantSamePositionNoMatch = "21\t26960070\trs116645811\tG\tA\t.\t.\t.";
        Pipe same = new SameVariantPipe(catalogFile, false, true, -1);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));

        History next = (History) p.next();
       	assertEquals("rs116645811", JsonPath.compile("ID").read(next.get(9)));        	
    }
    
   /** 7b: Ref: Not Matched, Alt: is a subset and AlleleFlag=True.. then return EMPTY 
     * 
    */
   @Test 
   public void testAlleleFlagTrue2() throws IOException{
   	// Ref: Not Matched, Alt: is a subset and AlleleFlag=True.. then return EMPTY
   	String variantSamePositionNoMatch = "21\t26960070\trs116645811\tX\tA\t.\t.\t.";
       Pipe same = new SameVariantPipe(catalogFile, false, true, -1);
       Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
       p.setStarts(Arrays.asList(variantSamePositionNoMatch));

       History next = (History) p.next();
       assertEquals("{}", next.get(9));        	
   }
   
  /** 7c: Ref: Matched, Alt: is NOT a subset and AlleleFlag=True.. then return EMPTY 
  */
  @Test 
  public void testAlleleFlagTrue3() throws IOException{
  	// Ref: Matched, Alt: is NOT a subset and AlleleFlag=True.. then return EMPTY
  	String variantSamePositionNoMatch = "21	34022588	rs115683257	C	G,T	.	.	.";
      Pipe same = new SameVariantPipe(catalogFile, false, true, -1);
      Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
      p.setStarts(Arrays.asList(variantSamePositionNoMatch));

      History next = (History) p.next();
      assertEquals("{}", next.get(9));        	
  }
   
  /** 7d: 
   * FLASE, FALSE: Match by rsID or Allele 
  */
  @Test 
  public void testAlleleFlagFalse1() throws IOException{
	  // RSID: MATCH - Ref: Matched, Alt: NOT a subset and AlleleFlag=True.. then return Row, since RSID matches
  	  String variantSamePositionNoMatch = "21	34022588	rs115683257	C	G	.	.	.";
  	  Pipe same = new SameVariantPipe(catalogFile, false, false, -1);
      Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
      p.setStarts(Arrays.asList(variantSamePositionNoMatch));

      History next = (History) p.next();
      assertEquals("rs115683257", JsonPath.compile("ID").read(next.get(9)));
  }

  /** 7e: 
   * FLASE, FALSE: Match by rsID or Allele 
  */
  @Test 
  public void testAlleleFlagFalse2() throws IOException{
	  // RSID: MATCH - Ref: Not Matched, Alt: NOT a subset and AlleleFlag=True.. then return Row, since RSID matches
  	  String variantSamePositionNoMatch = "21	34022588	rs115683257	T	G	.	.	.";
  	  Pipe same = new SameVariantPipe(catalogFile, false, false, -1);
      Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
      p.setStarts(Arrays.asList(variantSamePositionNoMatch));

      History next = (History) p.next();
      assertEquals("rs115683257", JsonPath.compile("ID").read(next.get(9)));
  }


  /** 7f: 
   * FLASE, FALSE: Match by rsID or Allele 
  */
  @Test 
  public void testAlleleFlagFalse3() throws IOException{
	  // RSID: NO MATCH - Ref: Matched, Alt: Is a subset and AlleleFlag=True.. then return Row, since REF matches
  	  String variantSamePositionNoMatch = "21	34022588	BADRSID	C	A,C	.	.	.";
  	  Pipe same = new SameVariantPipe(catalogFile, false, false, -1);
      Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
      p.setStarts(Arrays.asList(variantSamePositionNoMatch));

      History next = (History) p.next();
      assertEquals("rs115683257", JsonPath.compile("ID").read(next.get(9)));
  }

  /** 7g: 
   * FLASE, FALSE: Match by rsID or Allele 
  */
  @Test 
  public void testAlleleFlagFalse4() throws IOException{
	  // RSID: NO MATCH - Ref: Matched, Alt: NOT a subset and AlleleFlag=True.. then return EMPTY
  	  String variantSamePositionNoMatch = "21	34022588	BADRSID	C	G	.	.	.";
  	  Pipe same = new SameVariantPipe(catalogFile, false, false, -1);
      Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
      p.setStarts(Arrays.asList(variantSamePositionNoMatch));

      History next = (History) p.next();
      assertEquals("{}", next.get(9));
  }

  
  /****************************************************
     * 8. If both flags are set to TRUE, then throw exception
     */
    @Test (expected=IllegalArgumentException.class)
    public void testFlagsSetToTrue() throws IOException{
    	// RSID: NOT MATCHED and rsIDFlag=FALSE.. then return ROWif found baed on chr, pos
    	String variantSamePositionNoMatch = "DOESNT MATTER";
        
    	Pipe same = new SameVariantPipe(catalogFile, true, true, -1);
    }

    
    /********************************************************************************88
     * 9. Test for ALT:
     * 		in the file: C, T
     * 		input: A
     *      Output options: 
     *      	A -> C : OK; 
     *      	A -> T : OK; 
     *      	A -> [C,T] : OK
     *      	A -> any other than C, T.. FAIL
     */
    @Test
    public void testAltAlleleSubset1() throws IOException {
    	//ALT: in file "A,C" .. giving Subset: "A"
    	String variantSamePositionNoMatch = "21	34022588	rs115683257	C	A	.	.	.";
        Pipe same = new SameVariantPipe(catalogFile);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));

        History next = (History) p.next();
    	assertEquals("A,C", JsonPath.compile("ALT").read(next.get(9)));
    }

    /**
     * 9a. 
	*/
    @Test
    public void testAltAlleleSubset2() throws IOException {
        //ALT: in file "A,C" .. giving Subset: "C"
    	String variantSamePositionNoMatch = "21	34022588	rs115683257	C	C	.	.	.";
        Pipe same = new SameVariantPipe(catalogFile);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));
        History next = (History) p.next();
    	assertEquals("A,C", JsonPath.compile("ALT").read(next.get(9)));
    }

    /**
     * 9b. 
	*/
    @Test
    public void testAltAlleleFullList1() throws IOException {
        //ALT: in file "T,C" .. Ref: C ; Alt: "T,C"
    	String variantSamePositionNoMatch = "21\t34058146\trs114942253\tC\tT,C\t.\t.\t.";
        Pipe same = new SameVariantPipe(catalogFile, false, true, -1);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));
        History next = (History) p.next();
        assertEquals("T,C", JsonPath.compile("ALT").read(next.get(9)));        	
    }

    /**
     * 9c. 
	*/
    @Test
    public void testAltAlleleFullList2() throws IOException {
        //ALT: in file "T,C" .. Ref: T ; Alt: "T,C"
    	String variantSamePositionNoMatch = "21\t34058146\trs114942253\tT\tT,C\t.\t.\t.";
        Pipe same = new SameVariantPipe(catalogFile, false, true, -1);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));
        History next = (History) p.next();
        assertEquals("{}", next.get(9));        	
    }
    
    /**
     * 9d. 
	*/
    @Test
    public void testAltAlleleFullList3() throws IOException {
        //ALT: in file "T,C" .. Ref: C ; Alt: "T" -- 2 matches
    	String variantSamePositionNoMatch = "21\t34058146\trs114942253\tC\tT\t.\t.\t.";
        Pipe same = new SameVariantPipe(catalogFile, false, true, -1);
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), same);
        p.setStarts(Arrays.asList(variantSamePositionNoMatch));
        int resultCount=0;
        while(p.hasNext()) {
        	History next = (History) p.next();        	
        	resultCount++;
        }
        assertEquals(2, resultCount);        	
    }    

    /**
     * 
     * Test case based on error found by Greg.  1st data row has no match, 2nd row has a match.
     * 
     * @throws IOException
     */
    @Test
    public void testNoMatchFollowedByMatch() throws IOException{
        String variantNoMatch	= "99	00000000	rs?????????	G	A	.	.	.";
        String variantOneMatch	= "21	26960070	rs116645811	G	A	.	.	.";

        Pipeline<String, History> p = new Pipeline<String, History>
        (
        		new HistoryInPipe(), 
        		new VCF2VariantPipe(), 
        		new SameVariantPipe(catalogFile),
        		new DrillPipe(false, new String[] {"_id"})
        );
        
        p.setStarts(Arrays.asList(variantNoMatch, variantOneMatch));
        
        History history;
        
        assertTrue(p.hasNext());
        history = p.next();
        assertEquals(".",			history.get(history.size() - 1));
        
        assertTrue(p.hasNext());
        history = p.next();
        assertEquals("rs116645811",	history.get(history.size() - 1));
    }
    
    // ===================================================================
    // Test pipe where we choose the column in the history where the variant JSON comes from:
    // col 1 with only 1 column
    // col positive with multiple columns
    // col -1 with only 1 column
    // col -1 with multiple columns
    // col 0 - should throw error
    // ===================================================================
    
    /** Test column 1 as parm with only 1 column in input */
    @Test
    public void testColFlag_c1_1Col() throws IOException{
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), 
        	new HCutPipe( new int[] {1,2,3,4,5,6,7,8} ), new SameVariantPipe(catalogFile, 1));
        p.setStarts(getOneMatchInput());
        List<String> actual = PipeTestUtils.getResults(p);
        PipeTestUtils.assertListsEqual(getOneMatchExpected2Col(), actual);
    }

    
    
    /** Test positive column # as parm with multiple columns in input */
    @Test
    public void testColFlag_cPositive_MultiCols() throws IOException{
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), new SameVariantPipe(catalogFile, 9));
        p.setStarts(getOneMatchInput());
        List<String> actual = PipeTestUtils.getResults(p);
        PipeTestUtils.assertListsEqual(getOneMatchExpected(), actual);
    }

    
    /** Test column -1 as parm with only 1 column in input */
    @Test
    public void testColFlag_cNeg1_1Col() throws IOException{
    	Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), 
    		new HCutPipe( new int[] {1,2,3,4,5,6,7,8} ), new SameVariantPipe(catalogFile, -1));
        p.setStarts(getOneMatchInput());
        List<String> actual = PipeTestUtils.getResults(p);
        PipeTestUtils.assertListsEqual(getOneMatchExpected2Col(), actual);
    }

    
    /** Test column -1 as parm with multiple input columns */
    @Test
    public void testColFlag_cNeg1_MultiCols() throws IOException{
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), new SameVariantPipe(catalogFile, -1));
        p.setStarts(getOneMatchInput());
        List<String> actual = PipeTestUtils.getResults(p);
        PipeTestUtils.assertListsEqual(getOneMatchExpected(), actual);
    }

    /** Test column 0 as parm with multiple input columns - should throw exception */
    @Test (expected=InvalidPipeInputException.class)
    public void testColFlag_c0_MultiCols() throws IOException{
        Pipeline p = new Pipeline(new HistoryInPipe(), new VCF2VariantPipe(), new SameVariantPipe(catalogFile, 0));
        p.setStarts(getOneMatchInput());
        List<String> actual = PipeTestUtils.getResults(p);
        fail("Should not make it here - an exception should be thrown before getting this far!");
    }
    
    private List<String> getOneMatchInput() {
    	return Arrays.asList("21	26960070	rs116645811	G	A	.	.	.");
    }
    
    private List<String> getOneMatchExpected() {
        List<String> expected = Arrays.asList("21	26960070	rs116645811	G	A	.	.	.	"
            	+ "{\"CHROM\":\"21\",\"POS\":\"26960070\",\"ID\":\"rs116645811\",\"REF\":\"G\",\"ALT\":\"A\",\"QUAL\":\".\",\"FILTER\":\".\",\"INFO\":{\".\":true},\"_id\":\"rs116645811\",\"_type\":\"variant\",\"_landmark\":\"21\",\"_refAllele\":\"G\",\"_altAlleles\":[\"A\"],\"_minBP\":26960070,\"_maxBP\":26960070}	"
           		+ "{\"CHROM\":\"21\",\"POS\":\"26960070\",\"ID\":\"rs116645811\",\"REF\":\"G\",\"ALT\":\"A\",\"QUAL\":\".\",\"FILTER\":\".\",\"INFO\":{\".\":true},\"_id\":\"rs116645811\",\"_type\":\"variant\",\"_landmark\":\"21\",\"_refAllele\":\"G\",\"_altAlleles\":[\"A\"],\"_minBP\":26960070,\"_maxBP\":26960070}");
        return expected;
    }
    private List<String> getOneMatchExpected2Col() {
        List<String> expected = Arrays.asList(
            	  "{\"CHROM\":\"21\",\"POS\":\"26960070\",\"ID\":\"rs116645811\",\"REF\":\"G\",\"ALT\":\"A\",\"QUAL\":\".\",\"FILTER\":\".\",\"INFO\":{\".\":true},\"_id\":\"rs116645811\",\"_type\":\"variant\",\"_landmark\":\"21\",\"_refAllele\":\"G\",\"_altAlleles\":[\"A\"],\"_minBP\":26960070,\"_maxBP\":26960070}	"
           		+ "{\"CHROM\":\"21\",\"POS\":\"26960070\",\"ID\":\"rs116645811\",\"REF\":\"G\",\"ALT\":\"A\",\"QUAL\":\".\",\"FILTER\":\".\",\"INFO\":{\".\":true},\"_id\":\"rs116645811\",\"_type\":\"variant\",\"_landmark\":\"21\",\"_refAllele\":\"G\",\"_altAlleles\":[\"A\"],\"_minBP\":26960070,\"_maxBP\":26960070}");
        return expected;
    }
    
}
