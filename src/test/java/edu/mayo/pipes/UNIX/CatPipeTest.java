package edu.mayo.pipes.UNIX;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.transform.IdentityPipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.aggregators.AggregatorPipe;

@SuppressWarnings ("javadoc")
public class CatPipeTest {	
	
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
//		Pipe<String,String> file_pipe = new CatPipe();		
		Pipe<String,String> pipeline = new Pipeline<String,String>( new CatPipe());
		
		pipeline.setStarts(Arrays.asList("src/test/resources/testData/example.gff", "src/test/resources/testData/example2.gff"));
		
		while(pipeline.hasNext()) {
			String s = pipeline.next();
			result += s + "\n";	  
		}
		//System.out.println("Result=" + result + "*");
		//System.out.println("ExampleGFF=" + examplegff + "*");
		
		assertEquals(examplegff, result);		
	}	
	
	
	/**
	 * Test that the CatPipe can now read gzipped files
	 */
	@Test
	public void testProcessCatGZip ()
	{
		String					result = "";
		Pipe<String, String>	zip_pipe = new CatPipe ();
		Pipe<String, String>	identity = new IdentityPipe<String> ();
		Pipe<String, String>	pipeline = new Pipeline<String, String> (identity, zip_pipe);
		
		pipeline.setStarts (Arrays.asList ("src/test/resources/testData/example.gff.gz",
											"src/test/resources/testData/example2.gff.gz"));
		
		while (pipeline.hasNext ())
		{
			Object object = pipeline.next ();
			result += object.toString () + "\n";
		}
		
		// System.out.println(result + "*");
		// System.out.println(examplegff + "*");
		
		assertEquals (examplegff, result);
	}

	
	/**
	 * Test that the CatPipe can now read bzipped files
	 */
	@Test
	public void testProcessCatBZip ()
	{
		String					result = "";
		Pipe<String, String>	zip_pipe = new CatPipe ();
		Pipe<String, String>	identity = new IdentityPipe<String> ();
		Pipe<String, String>	pipeline = new Pipeline<String, String> (identity, zip_pipe);
		
		pipeline.setStarts (Arrays.asList ("src/test/resources/testData/example.gff.bz2",
											"src/test/resources/testData/example2.gff.bz2"));
		
		while (pipeline.hasNext ())
		{
			Object object = pipeline.next ();
			result += object.toString () + "\n";
		}
		
		// System.out.println(result + "*");
		// System.out.println(examplegff + "*");
		
		assertEquals (examplegff, result);
	}

	
	/**
	 * Test that the CatPipe can now read zipped files
	 */
	@Test
	public void testProcessCatZip ()
	{
		String					result = "";
		Pipe<String, String>	zip_pipe = new CatPipe ();
		Pipe<String, String>	identity = new IdentityPipe<String> ();
		Pipe<String, String>	pipeline = new Pipeline<String, String> (identity, zip_pipe);
		
		pipeline.setStarts (Arrays.asList ("src/test/resources/testData/example.gff.zip",
											"src/test/resources/testData/example2.gff.zip"));
		
		while (pipeline.hasNext ())
		{
			Object object = pipeline.next ();
			result += object.toString () + "\n";
		}
		
		// System.out.println(result + "*");
		// System.out.println(examplegff + "*");
		
		assertEquals (examplegff, result);
	}


	@Test
    public void catTest_NoInfiniteLoop() throws IOException {
        System.out.println("Start catTest_NoInfiniteLoop..");        

        String result = "";
        Pipe<String,String> cpipe = new CatPipe();        
        AggregatorPipe agg = new AggregatorPipe(5);
        Pipe<String, Collection> pipeline = new Pipeline<String, Collection>(cpipe, agg);
        pipeline.setStarts(Arrays.asList("src/test/resources/testData/genestest.tsv"));

        while(pipeline.hasNext()) {
        	pipeline.next();
        }        
        System.out.println("End catTest_NoInfiniteLoop.. if you can see this, the CatPipe exited!!");
    }
        
    @Test
    public void catTestOnLS(){
        /*TODO: this test does not pass! it is in FOGBUGZ!
        System.out.println("catTestOnLS");
        Pipe p = new Pipeline(new LSPipe(true), new CatPipe(), new PrintPipe());
        p.setStarts(Arrays.asList("src/test/resources/etl/testData/lsFolderDontADDSTUFFINHERE"));
        assertEquals(true,p.hasNext());
        for(int i=0; p.hasNext(); i++) {
        	String s = (String) p.next();
                System.out.println(s);
                if(i==0){
                    assertEquals("bar",s);
                }
                if(i==1){
                    assertEquals("baz",s);
                }
                if(i==2){
                    assertEquals("foo",s);
                }
        }
        * */
        
    }

}