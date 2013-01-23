/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics;

import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class FastaPipeTest {
    
    public FastaPipeTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    String fasta1 = ">SEQUENCE_1\tMTEITAAMVKELRESTGAGMMDCKNALSETNGDFDKAVQLLREKGLGKAAKKADRLAAEGLVSVKVSDDFTIAAMRPSYLSYEDLDMTFVENEYKALVAELEKENEERRRLKDPNKPEHKIPQFASRKQLSDAILKEAEEKIKEELKAQGKPEKIWDNIIPGKMNSFIADNSQLDSKLTLMGQFYVMDDKKTVEQVIAEKEKEFGGKIKIVEFICFEVGEGLEKKTEDFAAEVAAQL";
    String fasta2 = ">SEQUENCE_2\tSATVSEINSETDFVAKNDQFIALTKDTTAHIQSNSLQSVEELHSSTINGVKFEEYLKSQIATIGENLVVRRFATLKAGANGVVNGYIHTNGRVGVVIAAACDSAEVASKSRDLLRQICMH";
    public String fafile = "src/test/resources/testData/example.fa";
    /**
     * Test of processNextStart method, of class FastaPipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("processNextStart");
        Pipeline p = new Pipeline(new CatPipe(), 
                                new FastaPipe()
                                );
        p.setStarts(Arrays.asList(fafile));
        for(int i=0;p.hasNext();i++){
            String s = (String) p.next();
            if(i==0){
                assertEquals(fasta1, s);
            }
            if(i==2){
                assertEquals(fasta2, s);
            }
            
        }

    }
}
