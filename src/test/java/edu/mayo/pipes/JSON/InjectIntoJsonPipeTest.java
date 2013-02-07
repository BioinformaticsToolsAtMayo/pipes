package edu.mayo.pipes.JSON;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.JSON.inject.ColumnArrayInjector;
import edu.mayo.pipes.JSON.inject.ColumnInjector;
import edu.mayo.pipes.JSON.inject.Injector;
import edu.mayo.pipes.JSON.inject.JsonType;
import edu.mayo.pipes.JSON.inject.LiteralInjector;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.history.HistoryOutPipe;

public class InjectIntoJsonPipeTest {
	@Test
	public void stringValue() throws Exception {
		
		Injector injector = new ColumnInjector(1, JsonType.STRING);
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, injector);
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
	public void arrayValues() throws Exception {
		Injector[] injectors = new Injector[]
				{
					new ColumnArrayInjector(4, JsonType.STRING, ","),
					new ColumnArrayInjector(5, JsonType.NUMBER, "\\|"),
					new ColumnArrayInjector(6, JsonType.BOOLEAN, ","),
					new ColumnArrayInjector(7, JsonType.STRING, ",")
				};
		
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(8, injectors);
    	List<String> in = Arrays.asList( 
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tSTR_ARRAY_COL\tINT_ARRAY_COL\tBOOL_ARRAY_COL\tEMPTY_ARRAY_COL\tJSON",
				"chr17\t100\t101\tA,B,C\t1|2|3\ttrue,false,true\t\t{\"info\":\"somejunk\"}"
				);
		List<String> out = getPipeOutput(injectorPipe, in);
		in.set(2, "chr17\t100\t101\tA,B,C\t1|2|3\ttrue,false,true\t\t{\"info\":\"somejunk\",\"STR_ARRAY_COL\":[\"A\",\"B\",\"C\"],\"INT_ARRAY_COL\":[1,2,3],\"BOOL_ARRAY_COL\":[true,false,true],\"EMPTY_ARRAY_COL\":[]}" );
		assertListsEqual( in, out );
	}
	
	@Test
	public void colAsInt() throws Exception {
		Injector injector = new ColumnInjector(3, JsonType.NUMBER);
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, injector);
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
		Injector injector = new ColumnInjector(3, JsonType.NUMBER);
		
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, injector);
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
		Injector[] injectors = new Injector[]
				{
					new ColumnInjector(2, JsonType.NUMBER),
					new ColumnInjector(3, JsonType.NUMBER)
				};

		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, injectors);
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
		Injector injector = new ColumnInjector(2, JsonType.NUMBER);
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, injector);
    	List<String> in = Arrays.asList( 
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		List<String> out = getPipeOutput(injectorPipe, in);
		List<String> expected = Arrays.asList(
				"#UNKNOWN_1\tUNKNOWN_2\tUNKNOWN_3\tUNKNOWN_4",
				"chr17\t100\t101\t{\"info\":\"somejunk\",\"UNKNOWN_2\":100}"  );
		assertListsEqual( expected, out );
	}

	@Test
	/** User specifies both a numeric column as well as a key to use (instead of looking up the header on the header line). */
	public void headerSpecified() throws Exception {
		Injector injector = new ColumnInjector(1, "MyChromosome", JsonType.STRING);
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, injector);
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
	/** User specifies both a numeric column as well as a key to use (instead of looking up the header on the header line). */
	public void headerSpecifiedButNoHeaderLinePresent() throws Exception {
		Injector injector = new ColumnInjector(1, "MyChromosome", JsonType.STRING);
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, injector);
    	List<String> in = Arrays.asList( 
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		List<String> out = getPipeOutput(injectorPipe, in);
		List<String> expected = Arrays.asList(
				"#UNKNOWN_1\tUNKNOWN_2\tUNKNOWN_3\tUNKNOWN_4",
				"chr17\t100\t101\t{\"info\":\"somejunk\",\"MyChromosome\":\"chr17\"}");
		assertListsEqual( expected, out );
	}

	@Test
	/** User specifies both a numeric column as well as a key to use (instead of looking up the header on the header line). */
	public void headerSpecifiedButNoHeaderLinePresentAndCreateNewJsonCol() throws Exception {
		Injector injector = new ColumnInjector(1, "MyChromosome", JsonType.STRING);

		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(5, injector);
    	List<String> in = Arrays.asList( 
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		List<String> out = getPipeOutput(injectorPipe, in);
		List<String> expected = Arrays.asList(
				"#UNKNOWN_1\tUNKNOWN_2\tUNKNOWN_3\tUNKNOWN_4\tbior_injectIntoJson",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}\t{\"MyChromosome\":\"chr17\"}");
		assertListsEqual( expected, out );
	}

	@Test
	public void noJsonColumnSpecified() throws Exception {
		Injector[] injectors = new Injector[]
				{
					new LiteralInjector("MyKey", "MyValue", JsonType.STRING),
					new ColumnInjector(1, "Chromosome", JsonType.STRING)
				};

		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(injectors);
    	List<String> in = Arrays.asList( 
				"## Some unneeded header line",
				"#Chrom\tMinBP\tMaxBP\tJSON",
				"chr17\t100\t101\t{\"info\":\"somejunk\"}"
		);
		List<String> out = getPipeOutput(injectorPipe, in);
		in.set(2, "chr17\t100\t101\t{\"info\":\"somejunk\",\"MyKey\":\"MyValue\",\"Chromosome\":\"chr17\"}");
		assertListsEqual( in, out );
	}

	@Test
	public void keyAndValueSpecified() throws Exception {
		Injector[] injectors = new Injector[]
				{
					new LiteralInjector("MyKey", "MyValue", JsonType.STRING),
					new ColumnInjector(1, "Chromosome", JsonType.STRING)
				};
		
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, injectors);
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
		Injector injector = new ColumnInjector(4, "DuplicateJsonColShouldFail", JsonType.STRING);
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(4, injector);
		fail("An exception should have been thrown on the previous line");
	}
	
	@Test
	/** If the JSON column that we are inserting into doesn't exist, we should create and populate it */
	public void jsonColDoesNotExist() {
		Injector[] injectors = new Injector[]
				{
					new LiteralInjector("MyKey", "MyValue", JsonType.STRING),
					new ColumnInjector(1, "Chromosome", JsonType.STRING)
				};
		
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(5, injectors);
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
	public void jsonColDoesNotExistAndNoHeaderRows_multipleInputLines() {
		Injector[] injectors = new Injector[]
				{
					new LiteralInjector("MyKey", "MyValue", JsonType.STRING),
					new ColumnInjector(1, JsonType.STRING)
				};
		
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(5, injectors);
    	List<String> in = Arrays.asList( 
				"chr17\t100\t101\t{}",
				"chr18\t200\t201\t{}"
		);
		List<String> out = getPipeOutput(injectorPipe, in);
		List<String> expected = Arrays.asList(
				"#UNKNOWN_1\tUNKNOWN_2\tUNKNOWN_3\tUNKNOWN_4\t" + InjectIntoJsonPipe.NEW_JSON_HEADER,
				"chr17\t100\t101\t{}\t{\"MyKey\":\"MyValue\",\"UNKNOWN_1\":\"chr17\"}",
				"chr18\t200\t201\t{}\t{\"MyKey\":\"MyValue\",\"UNKNOWN_1\":\"chr18\"}"
				);
		assertListsEqual( expected, out );
	}
	
	@Test
	/** User specifies the names of some columns, but not the JSON column index */
	public void onlyColumnNamesSpecified_noHeaders() {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe("MyChrom", "Min", "Max");
    	List<String> in = Arrays.asList( "chr17\t100\t101\t{}" );
		List<String> out = getPipeOutput(injectorPipe, in);
		List<String> expected = Arrays.asList(
				"#UNKNOWN_1\tUNKNOWN_2\tUNKNOWN_3\tUNKNOWN_4",
				"chr17\t100\t101\t{\"MyChrom\":\"chr17\",\"Min\":\"100\",\"Max\":\"101\"}" );
		assertListsEqual( expected, out );
	}

	
	@Test
	/** User specifies the names of some columns, but not the JSON column index */
	public void onlyTwoOfFourColumnNamesSpecified_noHeaders() {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe("MyChrom", "Min");
    	List<String> in = Arrays.asList( "chr17\t100\t101\t{}" );
		List<String> out = getPipeOutput(injectorPipe, in);
		List<String> expected = Arrays.asList(
				"#UNKNOWN_1\tUNKNOWN_2\tUNKNOWN_3\tUNKNOWN_4",
				"chr17\t100\t101\t{\"MyChrom\":\"chr17\",\"Min\":\"100\"}" );
		System.out.println("expected: " + expected);
		System.out.println("actual:   " + out);
		assertListsEqual( expected, out );
	}

	@Test (expected = IndexOutOfBoundsException.class)
	/** User specifies the names of some columns, but not the JSON column index */
	public void tooManyColumnNamesSpecified_noHeaders() {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe("MyChrom", "Min", "Max", "EmptyJson", "JunkCol");
    	List<String> in = Arrays.asList( "chr17\t100\t101\t{}" );
		List<String> out = getPipeOutput(injectorPipe, in);
		List<String> expected = Arrays.asList(
				"#UNKNOWN_1\tUNKNOWN_2\tUNKNOWN_3\tUNKNOWN_4",
				"chr17\t100\t101\t{\"MyChrom\":\"chr17\",\"Min\":100}" );
		System.out.println("tooManyColumnNamesSpecified_noHeaders()");
		System.out.println("expected: " + expected);
		System.out.println("actual:   " + out);
		assertListsEqual( expected, out );
	}

	@Test
	/** User specifies the names of some columns, but not the JSON column index */
	public void justRightNumColumnNamesSpecified_noHeaders_addToEmptyJson() {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe("MyChrom", "Min", "Max");
    	List<String> in = Arrays.asList( "chr17\t100\t101\t{}" );
		List<String> out = getPipeOutput(injectorPipe, in);
		List<String> expected = Arrays.asList(
				"#UNKNOWN_1\tUNKNOWN_2\tUNKNOWN_3\tUNKNOWN_4",
				"chr17\t100\t101\t{\"MyChrom\":\"chr17\",\"Min\":\"100\",\"Max\":\"101\"}" );
		System.out.println("\njustRightNumColumnNamesSpecified_noHeaders_addToEmptyJson()");
		System.out.println("expected: " + expected);
		System.out.println("actual:   " + out);
		assertListsEqual( expected, out );
	}

	@Test
	/** User specifies the names of some columns, but not the JSON column index */
	public void justRightNumColumnNamesSpecified_noHeaders_addToNonEmptyJson() {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe("MyChrom", "Min", "Max");
    	List<String> in = Arrays.asList( "chr17\t100\t101\t{\"key\":\"value\"}" );
		List<String> out = getPipeOutput(injectorPipe, in);
		List<String> expected = Arrays.asList(
				"#UNKNOWN_1\tUNKNOWN_2\tUNKNOWN_3\tUNKNOWN_4",
				"chr17\t100\t101\t{\"key\":\"value\",\"MyChrom\":\"chr17\",\"Min\":\"100\",\"Max\":\"101\"}" );
		System.out.println("\njustRightNumColumnNamesSpecified_noHeaders_addToNonEmptyJson()");
		System.out.println("expected: " + expected);
		System.out.println("actual:   " + out);
		assertListsEqual( expected, out );
	}

	@Test
	/** User specifies the names of some columns, but not the JSON column index */
	public void justRightNumColumnNamesSpecified_noHeaders_addNewJsonCol() {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(true, "MyChrom", "Min", "Max");
    	List<String> in = Arrays.asList( "chr17\t100\t101" );
		List<String> out = getPipeOutput(injectorPipe, in);
		List<String> expected = Arrays.asList(
				"#UNKNOWN_1\tUNKNOWN_2\tUNKNOWN_3\t" + InjectIntoJsonPipe.NEW_JSON_HEADER,
				"chr17\t100\t101\t{\"MyChrom\":\"chr17\",\"Min\":\"100\",\"Max\":\"101\"}" );
		System.out.println("\njustRightNumColumnNamesSpecified_noHeaders_addNewJsonCol()");
		System.out.println("expected: "); printList(expected);
		System.out.println("actual:   "); printList(out);
		assertListsEqual( expected, out );
	}

	@Test (expected = IllegalArgumentException.class)
	/** User specifies the names of some columns, but not the JSON column index */
	public void justRightNumColumnNamesSpecified_noHeaders_lastColNotJson() {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe("MyChrom", "Min", "Max");
    	List<String> in = Arrays.asList( "chr17\t100\t101" );
		List<String> out = getPipeOutput(injectorPipe, in);
	}

	
	@Test
	/** Columns that are empty or contain "." should NOT be added to the JSON column */
	public void emptyColumnsShouldNotBeAddedToJson() {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe("MyChrom", "Min", "Max");
    	List<String> in = Arrays.asList( "chr17\t\t.\t{}" );
		List<String> out = getPipeOutput(injectorPipe, in);
		List<String> expected = Arrays.asList(
				"#UNKNOWN_1\tUNKNOWN_2\tUNKNOWN_3\tUNKNOWN_4",
				"chr17\t\t.\t{\"MyChrom\":\"chr17\"}" );
		assertListsEqual( expected, out );
	}

	@Test
	/** For some reason, '>' chars were converted to unicode (\u003e).  Make sure this doesn't happen */
	public void gtCharShouldNotBeConvertedToUnicode() {
		InjectIntoJsonPipe injectorPipe = new InjectIntoJsonPipe(
				new ColumnInjector(1, "ColWithGtChar", JsonType.STRING)
				);
    	List<String> in = Arrays.asList( "c.571G>A\t{}" );
		List<String> out = getPipeOutput(injectorPipe, in);
		List<String> expected = Arrays.asList(
				"#UNKNOWN_1\tUNKNOWN_2",
				"c.571G>A\t{\"ColWithGtChar\":\"c.571G>A\"}" );
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
	
	private void printList(List<String> strList) {
		for(String str : strList) {
			System.out.println(str);
		}
	}
}
