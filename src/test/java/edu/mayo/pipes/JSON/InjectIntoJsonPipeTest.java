package edu.mayo.pipes.JSON;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.history.HistoryOutPipe;

public class InjectIntoJsonPipeTest {
	@Test
	public void stringValue() throws Exception {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, new SimpleEntry("1",null));
    	List<String> in = Arrays.asList( 
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tJSON",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
				);
		List<String> out = getPipeOutput(injectorPipe, in);
		in.set(2, "chr17\t100\t101\t{\"info\":\"somejunk\",\"Chrom\":\"chr17\"}" );
		assertListsEqual( in, out );
	}
	
	@Test
	public void colAsInt() throws Exception {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, new SimpleEntry(3,null));
    	List<String> in = Arrays.asList( 
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tJSON",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		List<String> out = getPipeOutput(injectorPipe, in);
		in.set(2, "chr17\t100\t101\t{\"info\":\"somejunk\",\"MaxBP\":101}");
		assertListsEqual( in, out );
	}

	@Test
	public void colAsIntStr() throws Exception {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, new SimpleEntry("3",null));
    	List<String> in = Arrays.asList( 
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tJSON",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		List<String> out = getPipeOutput(injectorPipe, in);
		in.set(2, "chr17\t100\t101\t{\"info\":\"somejunk\",\"MaxBP\":101}");
		assertListsEqual( in, out );
	}


	@Test
	public void injectMultipleColumns() throws Exception {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, new SimpleEntry("2",null), new SimpleEntry("3", ""));
    	List<String> in = Arrays.asList( 
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tJSON",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		List<String> out = getPipeOutput(injectorPipe, in);
		in.set(2, "chr17\t100\t101\t{\"info\":\"somejunk\",\"MinBP\":100,\"MaxBP\":101}");
		assertListsEqual( in, out );
	}
	
	@Test
	/** User specifies only the column to grab data from (no header key).
	 *  Header should be grabbed from header line.	 */
	public void noHeaderSpecifiedButHeaderLinePresent() throws Exception {
		// Same as stringValue() or numberValue();
		stringValue();
	}

	@Test
	/** User specifies only the column to grab data from (no header key).
	 *  Header should be grabbed from header line, only there is no header line present, so it should be "(Unknown)"  */
	public void noHeaderSpecifiedAndNoHeaderLinePresent() throws Exception {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, new SimpleEntry("2",null));
    	List<String> in = Arrays.asList( 
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		List<String> out = getPipeOutput(injectorPipe, in);
		List<String> expected = Arrays.asList(
				"#UNKNOWN_1\tUNKNOWN_2\tUNKNOWN_3\tUNKNOWN_4",
				"chr17\t100\t101\t{\"info\":\"somejunk\",\"(Unknown)\":100}"  );
		assertListsEqual( expected, out );
	}

	@Test
	/** User specifies both a numeric column as well as a key to use (instead of looking up the header on the header line). */
	public void headerSpecified() throws Exception {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, new SimpleEntry("1","MyChromosome"));
    	List<String> in = Arrays.asList( 
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tJSON",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		List<String> out = getPipeOutput(injectorPipe, in);
		in.set(2, "chr17\t100\t101\t{\"info\":\"somejunk\",\"MyChromosome\":\"chr17\"}");
		assertListsEqual( in, out );
	}
	
	@Test
	public void keyAndValueSpecified() throws Exception {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, new SimpleEntry("MyKey","MyValue"), new SimpleEntry("1","Chromosome"));
    	List<String> in = Arrays.asList( 
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tJSON",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		List<String> out = getPipeOutput(injectorPipe, in);
		in.set(2, "chr17\t100\t101\t{\"info\":\"somejunk\",\"MyKey\":\"MyValue\",\"Chromosome\":\"chr17\"}");
		assertListsEqual( in, out );
	}

	
	@Test (expected = IllegalArgumentException.class)
	public void colAndJsonColSame() {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, new SimpleEntry("4","DuplicateJsonColShouldFail"));
		fail("An exception should have been thrown on the previous line");
	}
	
	@Test
	/** If the JSON column that we are inserting into doesn't exist, we should create and populate it */
	public void jsonColDoesNotExist() {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(5, new SimpleEntry("MyKey","MyValue"), new SimpleEntry("1","Chromosome"));
    	List<String> in = Arrays.asList( 
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tJSON",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		List<String> out = getPipeOutput(injectorPipe, in);
		in.set(1, in.get(1) + "\tbior_injectIntoJson");
		in.set(2, "chr17\t100\t101\t{\"info\":\"somejunk\"}\t{\"MyKey\":\"MyValue\",\"Chromosome\":\"chr17\"}");
		assertListsEqual( in, out );
	}
	
	@Test
	/** If the JSON column that we are inserting into doesn't exist, we should create and populate it */
	public void jsonColDoesNotExistAndNoHeaderRows() {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(5, new SimpleEntry("MyKey","MyValue"), new SimpleEntry(1,null));
    	List<String> in = Arrays.asList( 
				"chr17\t100\t101\t{}"
		);
		List<String> out = getPipeOutput(injectorPipe, in);
		List<String> expected = Arrays.asList(
				"#UNKNOWN_1\tUNKNOWN_2\tUNKNOWN_3\tUNKNOWN_4\t" + InjectIntoJsonPipe.NEW_JSON_HEADER,
				"chr17\t100\t101\t{}\t{\"MyKey\":\"MyValue\",\"(Unknown)\":\"chr17\"}" );
		assertListsEqual( expected, out );
	}
	
	
	//=========================================================================================
	// Helper methods
	//=========================================================================================

	private List<String> getPipeOutput(InjectIntoJsonPipe injectorPipe, List<String> in) {
        Pipe<String, String> pipe = new Pipeline(new HistoryInPipe(), injectorPipe, new HistoryOutPipe());
		pipe.setStarts(in);
		List<String> historyOut = new ArrayList<String>();
		while(pipe.hasNext()) {
			historyOut.add((String)pipe.next());
		}
		return historyOut;
	}
	
	private void assertListsEqual(List<String> expected, List<String> actual) {
		assertTrue( "Expected: " + expected + "\n  Actual: " + actual,  expected.equals(actual) );
	}
}
