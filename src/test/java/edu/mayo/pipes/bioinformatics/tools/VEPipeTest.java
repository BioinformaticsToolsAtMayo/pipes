/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics.tools;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.UNIX.GrepEPipe;
import edu.mayo.pipes.UNIX.GrepPipe;
import edu.mayo.pipes.WritePipe;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class VEPipeTest {
    
    public VEPipeTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        vep = new VEPipe("/tmp/");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        //vep.close();
    }
    
    
    
    @Before
    public void setUp() {
        
    }
    
    @After
    public void tearDown() {
        //
    }


    public static VEPipe vep = null;

    /**
     * Test of processNextStart method, of class VEPipe.
     */
    @Test
    public void testProcessNextStart() {
        System.out.println("processNextStart");
        vep.reset(); 
        Pipe p = new Pipeline( new CatPipe(),new GrepEPipe("^#.*"), new SplitPipe("\t"), new PrintPipe());
        p.setStarts(Arrays.asList( "src/test/resources/testData/tools/vep/example.vcf" ));
        while(p.hasNext()){
            p.next();
        }
    }
    
    @Test
    public void testMakeInputFile() throws IOException{
        System.out.println("Test Make Input File ");
        vep.reset();
        Pipeline p = new Pipeline(vep);
        List<String> l1 = Arrays.asList("line1", "a", "b", "c");
        List<String> l2 = Arrays.asList("line2", "d", "e", "f");
        List<String> l3 = Arrays.asList("line3", "g", "h", "i");
        List<List<String>> lines = Arrays.asList(l1, l2, l3 );
        vep.setStarts(lines);
        vep.makeInputFile();
        
        //now cat the sucker...
        String inputfile = vep.getInputfile();
        System.out.println(inputfile);
        Pipe p2 = new Pipeline(new CatPipe(), new SplitPipe("\t"));
        p2.setStarts(Arrays.asList(inputfile));
        for(int i=0; p2.hasNext(); i++){
            //System.out.println(i);
            List<String> next = (List<String>) p2.next();
            if(i==0){
                assertEquals("line1",next.get(0));
            }
            if(i==1){
                assertEquals("line2",next.get(0));
            }
            if(i==2){
                assertEquals("line3",next.get(0));
            }
        }
        
        File file = new File(inputfile);
        assert(file.exists());
        //vep.exec("cat " + inputfile);
        
        
    }
        
    @Test
    public void testEXE() throws IOException, InterruptedException{
        System.out.println("testEXE");
        String outfile = vep.getOutfile();
        vep.exec("touch " + outfile);
        File file = new File(outfile);
        assert(file.exists());
        //put some data in the file with an exec...
        StringBuffer stdout = new StringBuffer();
        StringBuffer stderr = new StringBuffer();
        vep.exec("cat src/test/resources/testData/tools/vep/example.vcf", stdout, stderr );
        //System.out.println(stdout.toString());
        assertEquals(5798,stdout.length());
        assertEquals(0,stderr.length());
        FileWriter fstream = new FileWriter(outfile);
        BufferedWriter br = new BufferedWriter(fstream);
        br.write(stdout.toString());
        br.close();
        assertEquals(5798,file.length());
        //now test that standard error gets something...
        stdout = new StringBuffer();
        stderr = new StringBuffer();
        vep.exec("perl -e  'this is some perl code that does not run';", stdout, stderr);
        System.out.println("******** " + stderr.toString());
        assertEquals(0,stdout.length());
        assertEquals(67,stderr.length());
        
        
    }
    
    @Test
    public void testLoad(){
        System.out.println("testLoad");
        LinkedList loadfile = vep.loadfile("src/test/resources/testData/tools/vep/vep_example.txt");
        assertEquals(1000, loadfile.size());
        while(vep.hasNext()){
            List<String> next = vep.next();
            //System.out.println(next);
        }
        
    }

}
