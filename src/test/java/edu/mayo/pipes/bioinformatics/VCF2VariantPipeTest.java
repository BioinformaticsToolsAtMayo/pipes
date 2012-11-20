/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.HeaderPipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.UNIX.CatGZPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.aggregators.AggregatorPipe;
import edu.mayo.pipes.records.Variant;

import java.util.Arrays;
import org.junit.*;
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

    /**
     * Test of processNextStart method, of class VCF2VariantPipe.
     */
    @Test
    public void testProcessNextStart() {
        VCF2VariantPipe vcf = new VCF2VariantPipe();
        Pipe<String, Variant> pipeline = new Pipeline<String, Variant>(new CatPipe(),new HeaderPipe(59), new SplitPipe("\t"), vcf);
        pipeline.setStarts(Arrays.asList("/src/test/resources/testData/example.vcf"));
        if (pipeline.hasNext()) {
	        Variant v = (Variant)pipeline.next();
	        System.out.println(v);
	        assertEquals("chr1", v.getChr().toString());
	        //assertEquals("rs144773400", v.getRsID());
	        //assertEquals("137", v.getVersion());
	        //assertEquals("TA",v.getRefAlleleFWD());
	        //assertEquals("T", v.getAltAlleleFWD().get(0));
        }
	
    }
}
