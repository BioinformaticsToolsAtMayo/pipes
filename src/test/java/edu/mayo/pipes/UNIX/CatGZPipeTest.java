package edu.mayo.pipes.UNIX;



import edu.mayo.pipes.UNIX.CatGZPipe;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.transform.IdentityPipe;
import com.tinkerpop.pipes.util.Pipeline;

public class CatGZPipeTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {		
//        File zipped = new File("./src/test/resources/etl/testData/example.gff.gz");
//        File dir2 = new File(".");
//        try {
//            System.out.println("Current dir : " + zipped.getCanonicalPath());
//            System.out.println("Parent  dir : " + dir2.getCanonicalPath());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	private String examplegff = "ctg123\t.\tgene\t1300\t1500\t.\t+\t.\tID=gene00001;namespace=UCSC\n" +
			"ctg123\t.\tgene\t1050\t1500\t.\t+\t.\tID=gene00002;namespace=Ensembl;\n" +
			"ctg123\t.\tgene\t3000\t3902\t.\t+\t.\tID=gene00003;GO_id=1234\n" +
			"ctg123\t.\tgene\t5000\t5500\t.\t+\t.\tID=gene00004\n" +
			"ctg123\t.\tgene\t7000\t9000\t.\t+\t.\tID=gene00005\n" +
			"ctg123\t.\tgene\t1300\t1500\t.\t+\t.\tID=gene00006\n" +
			"ctg123\t.\tgene\t1050\t1500\t.\t+\t.\tID=gene00007\n" +
			"ctg123\t.\tgene\t3000\t3902\t.\t+\t.\tID=gene00008\n" +
			"ctg123\t.\tgene\t5000\t5500\t.\t+\t.\tID=gene00009\n" +
			"ctg123\t.\tgene\t7000\t9000\t.\t+\t.\tID=gene00010\n";

	@Test
	public void testProcessNextStart() {
		String result = "";
		Pipe<String,String> zip_pipe = new CatGZPipe("gzip");
		Pipe<String,String> identity = new IdentityPipe();
		Pipe<String,String> pipeline = new Pipeline<String,String>(identity, zip_pipe);
		pipeline.setStarts(Arrays.asList("src/test/resources/testData/example.gff.gz", "src/test/resources/testData/example2.gff.gz"));
		while(pipeline.hasNext()) {
			  Object object = pipeline.next();
			  result += object.toString() + "\n";	  
		}
		//System.out.println(result + "*");
		//System.out.println(examplegff + "*");
		
		assertEquals(examplegff, result);
		
	}

}
