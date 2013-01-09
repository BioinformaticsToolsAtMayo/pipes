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

import edu.mayo.pipes.history.History;

public class InjectIntoJsonPipeTest {
	@Test
	public void stringValue() throws Exception {
		InjectIntoJsonPipe pipe = new InjectIntoJsonPipe(4, new SimpleEntry("1",null));
		List<History> historyListIn = toHistoryList(
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tJSON",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		pipe.setStarts(historyListIn);
		List<History> historyListOut = getPipeOutput(pipe);
		assertEquals(3, historyListOut.size());
		assertEquals(historyListIn.get(0), historyListOut.get(0));
		assertEquals(historyListIn.get(1), historyListOut.get(1));
		assertHistoryEquals(
				toHistory("chr17\t100\t101\t{\"info\":\"somejunk\", \"Chrom\":\"chr17\"}"),
				historyListOut.get(2) );
	}
	


	@Test
	public void numberValue() throws Exception {
		// TODO: Why is it if I change the column to 1,2,or 3 it doesn't matter??????????????????????????????????????????????????????
		// TODO: It still passes
		InjectIntoJsonPipe pipe = new InjectIntoJsonPipe(4, new SimpleEntry("3",null));
		List<History> historyListIn = toHistoryList(
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tJSON",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		pipe.setStarts(historyListIn);
		List<History> historyListOut = getPipeOutput(pipe);
		assertEquals(3, historyListOut.size());
		assertEquals(historyListIn.get(0), historyListOut.get(0));
		assertEquals(historyListIn.get(1), historyListOut.get(1));
		assertHistoryEquals(
				toHistory("chr17\t100\t101\t{\"info\":\"somejunk\", \"MaxBP\":101}"),
				historyListOut.get(2) );
	}
	
	@Test
	public void injectMultipleColumns() throws Exception {
		InjectIntoJsonPipe pipe = new InjectIntoJsonPipe(4, new SimpleEntry("2",null), new SimpleEntry("3", ""));
		List<History> historyListIn = toHistoryList(
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tJSON",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		pipe.setStarts(historyListIn);
		List<History> historyListOut = getPipeOutput(pipe);
		assertEquals(3, historyListOut.size());
		assertEquals(historyListIn.get(0), historyListOut.get(0));
		assertEquals(historyListIn.get(1), historyListOut.get(1));
		assertHistoryEquals(
				toHistory("chr17\t100\t101\t{\"info\":\"somejunk\", \"MinBP\":100, \"MaxBP\":101}"),
				historyListOut.get(2) );
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
		InjectIntoJsonPipe pipe = new InjectIntoJsonPipe(4, new SimpleEntry("2",null));
		List<History> historyListIn = toHistoryList(
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		pipe.setStarts(historyListIn);
		List<History> historyListOut = getPipeOutput(pipe);
		assertEquals(1, historyListOut.size());
		assertHistoryEquals(
				toHistory("chr17\t100\t101\t{\"info\":\"somejunk\", \"(Unknown)\":100}"),
				historyListOut.get(0) );
	}

	@Test
	/** User specifies both a numeric column as well as a key to use (instead of looking up the header on the header line). */
	public void headerSpecified() throws Exception {
		InjectIntoJsonPipe pipe = new InjectIntoJsonPipe(4, new SimpleEntry("1","MyChromosome"));
		List<History> historyListIn = toHistoryList(
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tJSON",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		pipe.setStarts(historyListIn);
		List<History> historyListOut = getPipeOutput(pipe);
		assertEquals(3, historyListOut.size());
		assertEquals(historyListIn.get(0), historyListOut.get(0));
		assertEquals(historyListIn.get(1), historyListOut.get(1));
		assertHistoryEquals(
				toHistory("chr17\t100\t101\t{\"info\":\"somejunk\", \"MyChromosome\":\"chr17\"}"),
				historyListOut.get(2) );
	}
	
	@Test
	public void keyAndValueSpecified() throws Exception {
		InjectIntoJsonPipe pipe = new InjectIntoJsonPipe(4, new SimpleEntry("MyKey","MyValue"), new SimpleEntry("1","Chromosome"));
		List<History> historyListIn = toHistoryList(
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tJSON",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		pipe.setStarts(historyListIn);
		List<History> historyListOut = getPipeOutput(pipe);
		assertEquals(3, historyListOut.size());
		assertEquals(historyListIn.get(0), historyListOut.get(0));
		assertEquals(historyListIn.get(1), historyListOut.get(1));
		assertHistoryEquals(
				toHistory("chr17\t100\t101\t{\"info\":\"somejunk\", \"MyKey\":\"MyValue\", \"Chromosome\":\"chr17\"}"),
				historyListOut.get(2) );
	}

	
	@Test (expected = IllegalArgumentException.class)
	public void colAndJsonColSame() {
		InjectIntoJsonPipe pipe = new InjectIntoJsonPipe(4, new SimpleEntry("4","DuplicateJsonColShouldFail"));
		fail("An exception should have been thrown on the previous line");
	}
	
	@Test
	/** If the JSON column that we are inserting into doesn't exist, we should create and populate it */
	public void jsonColDoesNotExit() {
		InjectIntoJsonPipe pipe = new InjectIntoJsonPipe(5, new SimpleEntry("MyKey","MyValue"), new SimpleEntry("1","Chromosome"));
		List<History> historyListIn = toHistoryList(
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tJSON",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		pipe.setStarts(historyListIn);
		List<History> historyListOut = getPipeOutput(pipe);
		assertEquals(3, historyListOut.size());
		assertEquals(historyListIn.get(0), historyListOut.get(0));
		historyListIn.get(1).add("NewJson");
		assertEquals(historyListIn.get(1), historyListOut.get(1));
		assertHistoryEquals(
				toHistory("chr17\t100\t101\t{\"info\":\"somejunk\"}\t{\"MyKey\":\"MyValue\", \"Chromosome\":\"chr17\"}"),
				historyListOut.get(2) );
	}
	
	
	//=========================================================================================
	// Helper methods
	//=========================================================================================

	private List<History> toHistoryList(String... linesIn) {
		List<History> histList = new ArrayList<History>();
		for(String line : linesIn) {
			histList.add(toHistory(line));
		}
		return histList;
	}
	
	private History toHistory(String line) {
		String[] cols = line.split("\t");
		History hist = new History();
		for(String col : cols) {
			hist.add(col);
		}
		return hist;
	}
	
	private List<History> getPipeOutput(Pipe pipe) {
		List<History> historyOut = new ArrayList<History>();
		while(pipe.hasNext()) {
			historyOut.add((History)pipe.next());
		}
		return historyOut;
	}
	
	
	private boolean isHistoryEquals(History h1, History h2) {
		if(h2 == null && h2 == null )
			return true;
		else if(h1 == null || h2 == null)
			return false;
		else if(h1.size() != h2.size()) 
			return false;
		
		for(int i=0; i < h1.size(); i++) {
			if( ! h1.get(i).equals(h2.get(i)) )
				return false;
		}
		return true;
	}
	
	private void assertHistoryEquals(History expected, History actual) {
		if(expected == null && actual == null )
			return;
		else if(expected == null || actual == null)
			fail("One of the history objects is null while the other is not.  expected: " + expected + ", actual:   " + actual);
		else if(expected.size() != actual.size()) 
			fail("The two History objects are not the same size.  expected: " + expected.size() + ", actual:   " + actual.size());
		
		for(int i=0; i < expected.size(); i++) {
			if( ! expected.get(i).equals(actual.get(i)) )
				fail("Element #" + i + " is not the same:\n  expected[" + i + "]: " + expected.get(i) + "\n  actual[" + i + "]:   " + actual.get(i));
		}
		return;
	}

	
	private boolean isStringArraysEquals(String[] array1, String[] array2) {
		if(array1 == null && array2 == null )
			return true;
		else if(array1 == null || array2 == null)
			return false;
		else if(array1.length != array2.length) 
			return false;
		
		for(int i=0; i < array1.length; i++) {
			if( ! array1[i].equals(array2[i]) )
				return false;
		}
		return true;
	}
	
}
