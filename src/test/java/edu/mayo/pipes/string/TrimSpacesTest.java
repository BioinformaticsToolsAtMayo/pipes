package edu.mayo.pipes.string;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.util.test.PipeTestUtils;

/**
 * @author Michael Meiners (m054457)
 * Date created: Jan 13, 2014
 */
public class TrimSpacesTest {

	private final String EXPECTED = "A\tB\tC\tD\tE";
	
	@Test
	public void testNoSpaces() {
		compare("A\tB\tC\tD\tE", EXPECTED);
	}
	
	@Test
	public void testLine() {
		compare("A\t  B \tC\t    D\tE  ", EXPECTED);
	}
	
	@Test
	public void testSpaceInFrontAndEnd() {
		compare(" A\tB\tC\tD\tE  ", EXPECTED);
	}
	
	
	@Test
	public void testTwoAdjacentTabs() {
		compare("A\tB\t\tC\tD\tE", "A\tB\t\tC\tD\tE");
	}
	
	@Test
	public void testDontRemoveSpacesAndTabsFromMetadata() {
		List<String> INPUT = Arrays.asList(
				"##INFO=<DESCRIPTION=\"Some text with a \t tab in the middle\">  ",
				"#CHROM \t POS    \tID \tREF\tALT\t QUAL\tFILTER\tINFO",
				"1\t 200\trs111222  \tA\tG\t0\t0\tsomeInfo  " );
		List<String> EXPECTED = Arrays.asList(
				"##INFO=<DESCRIPTION=\"Some text with a \t tab in the middle\">  ",
				"#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
				"1\t200\trs111222\tA\tG\t0\t0\tsomeInfo" );
		compare(INPUT, EXPECTED);
	}

	private void compare(String in, String expected) {
        Pipeline p = new Pipeline(new TrimSpacesPipe());
        p.setStarts(Arrays.asList(in));
        List<String> actual = PipeTestUtils.getResults(p);
        PipeTestUtils.assertListsEqual(Arrays.asList(expected), actual);
	}

	private void compare(List<String> in, List<String> expected) {
        Pipeline p = new Pipeline(new TrimSpacesPipe());
        p.setStarts(in);
        List<String> actual = PipeTestUtils.getResults(p);
        PipeTestUtils.assertListsEqual(expected, actual);
	}

}
