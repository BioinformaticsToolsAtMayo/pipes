/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import java.util.Arrays;
import org.junit.*;
import static org.junit.Assert.*;
import edu.mayo.pipes.*;
import edu.mayo.pipes.UNIX.HeadPipe;

/**
 *
 * @author dquest
 */
public class HeadPipeTest {
    
    public HeadPipeTest() {
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

	private String examplegff = "ctg123\t.\tgene\t1300\t1500\t.\t+\t.\tID=gene00001;namespace=UCSC\n" +
			"ctg123\t.\tgene\t1050\t1500\t.\t+\t.\tID=gene00002;namespace=Ensembl;\n";

	@Test
	public void testProcessNextStart() {
		String result = "";System.err.println("test");
		HeadPipe file_pipe = new HeadPipe(2);
		Pipe<String,String> pipeline = new Pipeline<String,String>(file_pipe);
		pipeline.setStarts(Arrays.asList("./src/test/resources/etl/testData/example.gff", "./src/test/resources/etl/testData/example2.gff"));
		
                while(pipeline.hasNext()) {
			  String s = pipeline.next();
                          System.err.println(s);
			  result += s + "\n";	  
		}//TODO: there is some sort of bug if we want to head multiple files
		//System.out.println(result + "*");
		//System.out.println(examplegff + "*");
		
		assertEquals(examplegff, result);
		
	}
}
