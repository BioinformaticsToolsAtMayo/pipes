/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.exec.UnixStreamCommand;
import edu.mayo.pipes.UNIX.CatGZPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.aggregators.AggregatorPipe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
public class ExecPipeTest {
    
    public ExecPipeTest() {
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

    /**
     * Test of processNextStart method, of class ExecPipe.
     */
    //@Test
    public void testProcessNextStart() throws IOException, InterruptedException {
        System.out.println("processNextStart");
        String[] command = {"cat"};
        ExecPipe exe = new ExecPipe(command, true);
        Pipe p = new Pipeline(
                new AggregatorPipe(100),
                exe,
                new DrainPipe(),
                new PrintPipe()
                );
        List<String> asList = Arrays.asList("foo", "bar");
        p.setStarts(asList);
        for(int i=0;p.hasNext();i++){
            String result = (String) p.next();
            if(i==0){
                assertEquals("foo", result);
            }
            if(i==1){
                assertEquals("bar", result);
            }
        }
        exe.shutdown(); //MAKE SURE TO DO THIS EVERY TIME YOU USE AN EXECPIPE!!!
    }
    
        /**
     * Test of processNextStart method, of class ExecPipe.
     */
    //@Test
    public void testGrep() throws IOException {
        System.out.println("test grep via exec pipe");
        String[] command = {"grep", "bar"};
        ExecPipe exe = new ExecPipe(command, true);
        Pipe p = new Pipeline(
                new AggregatorPipe(100),
                exe,
                new DrainPipe(),
                new PrintPipe()
                );
        List<String> asList = Arrays.asList("foo", "bar");
        p.setStarts(asList);
        for(int i=0;p.hasNext();i++){
            String result = (String) p.next();
            if(i==0){
                assertEquals("bar", result);
            }
        }
        //assertEquals(expResult, result);

    }
    
    private String[] outputLines = new String[]
            {
        "12	112079950	112123790	{\"_type\":\"gene\",\"_landmark\":\"12\",\"_strand\":\"-\",\"_minBP\":112079950,\"_maxBP\":112123790,\"gene\":\"BRAP\",\"gene_synonym\":\"BRAP2; IMP; RNF52\",\"note\":\"BRCA1 associated protein; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"8315\",\"HGNC\":\"1099\",\"MIM\":\"604986\"}",
        "17	41196312	41277500	{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"-\",\"_minBP\":41196312,\"_maxBP\":41277500,\"gene\":\"BRCA1\",\"gene_synonym\":\"BRCAI; BRCC1; BROVCA1; IRIS; PNCA4; PPP1R53; PSCP; RNF53\",\"note\":\"breast cancer 1, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"672\",\"HGNC\":\"1100\",\"HPRD\":\"00218\",\"MIM\":\"113705\"}",
        "17	41277600	41297130	{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"+\",\"_minBP\":41277600,\"_maxBP\":41297130,\"gene\":\"NBR2\",\"gene_synonym\":\"NCRNA00192\",\"note\":\"neighbor of BRCA1 gene 2 (non-protein coding); Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"10230\",\"HGNC\":\"20691\"}"	
    };
    
    public ArrayList<String> getBiggo(){
        Pipe p = new Pipeline(
                new CatGZPipe("gzip")
                );
        p.setStarts(Arrays.asList("src/test/resources/testData/tabix/genes.tsv.bgz"));
        ArrayList<String> lines = new ArrayList<String>();
        for(int i=0;p.hasNext();i++){
            String result = (String) p.next();
            lines.add(result);
        }
        return lines;
    }
    
    //@Test
    public void testlargeGrep() throws IOException, InterruptedException {
        System.out.println("test large file grep via exec pipe");
        String[] command = {"grep", "BRCA1"};

        ArrayList<String> lines = getBiggo();

        //assertEquals(expResult, result);
        UnixStreamCommand cmd = new UnixStreamCommand(command, NO_CUSTOM_ENV, true,  UnixStreamCommand.StdoutBufferingMode.LINE_BUFFERED, 0);
        cmd.launch();

        List<String> output; 
        for(int k = 0; k<2; k++){//run at least two large datasets through the grep (do the file twice)
            cmd.send(lines);
            output = cmd.receive();
            for (int i=0; i < outputLines.length; i++) {
                //System.out.println(output.get(i));
                assertEquals(outputLines[i], output.get(i));
            }     
        }

        cmd.terminate();
    }
    
    private static final Map<String, String> NO_CUSTOM_ENV = Collections.emptyMap();
    
    
    //@Test
    public void testlargeGrepExe() throws IOException, InterruptedException {
        System.out.println("test large file grep via exec pipe");
        String[] command = {"grep", "--line-buffered", "BRCA1"};
        ExecPipe exe = new ExecPipe(command, true);
        Pipe p = new Pipeline(
                new CatGZPipe("gzip"),
                new AggregatorPipe(1),
                exe,
                new DrainPipe()
                //new PrintPipe()
                );
        p.setStarts(Arrays.asList("src/test/resources/testData/tabix/genes.tsv.bgz"));
        for(int i=0;p.hasNext();i++){
            String result = (String) p.next();
            System.out.println(i);
            //if(i==1) break;
        }
        exe.shutdown();

    }
    
    //@Test
    public void testlargeGrepExeShort() throws IOException, InterruptedException {
        System.out.println("test large file grep via exec pipe");
        String[] command = {"grep", "--line-buffered", "BRCA1"};
        ExecPipe exe = new ExecPipe(command, true);
        Pipe p = new Pipeline(
                //new CatPipe(),
                new AggregatorPipe(1),
                exe,
                new DrainPipe(),
                new PrintPipe()
                );
        ArrayList<String> foo = new ArrayList<String>();
        foo.addAll(Arrays.asList(outputLines[0],outputLines[1],outputLines[2]));
        p.setStarts(foo);
        //p.setStarts(Arrays.asList("src/test/resources/testData/example.gbk"));
        for(int i=0;p.hasNext();i++){
            String result = (String) p.next();
            System.out.println(i);
            //if(i==1) break;
        }
        exe.shutdown();

    }


}
