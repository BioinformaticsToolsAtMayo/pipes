package edu.mayo.pipes.util;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import edu.mayo.pipes.util.FieldSpecification;
import edu.mayo.pipes.util.FieldSpecification.FieldType;

public class FieldSpecificationTest
{
	
	@Test
	public void testNthOnlySingle()
	{
		List<Integer> match    = Arrays.asList(5);
		List<Integer> nonmatch = Arrays.asList(1, 2, 3, 4, 6);
		
		String spec = "5";
		
		validate(spec, match, nonmatch);
	}

	@Test
	public void testNthOnlyMultiple()
	{
		List<Integer> match    = Arrays.asList(2, 5, 7);
		List<Integer> nonmatch = Arrays.asList(1, 3, 4, 6);
		
		String spec = "2,5,7";
		
		validate(spec, match, nonmatch);
	}

	@Test
	public void testNthToEnd()
	{
		List<Integer> match    = Arrays.asList(5, 6, 7);
		List<Integer> nonmatch = Arrays.asList(1, 2, 3, 4);
		
		String spec = "5-";
		
		validate(spec, match, nonmatch);
	}	
	
	@Test
	public void testFirstToMth()
	{
		List<Integer> match    = Arrays.asList(1, 2, 3);
		List<Integer> nonmatch = Arrays.asList(4, 5, 6);
		
		String spec = "-3";
		
		validate(spec, match, nonmatch);
	}	

	@Test
	public void testNthToMth()
	{
		List<Integer> match    = Arrays.asList(2, 3, 4, 5);
		List<Integer> nonmatch = Arrays.asList(1, 6, 7);
		
		String spec = "2-5";
		
		validate(spec, match, nonmatch);
	}	
	
	private void validate(String spec, List<Integer> expectedMatch, List<Integer> expectedNonMatch)
	{
		FieldSpecification fSpec = new FieldSpecification(spec);
		
		int numFields = expectedMatch.size() + expectedNonMatch.size();
		
		Map<FieldType, List<Integer>> m = fSpec.getFields(numFields);
		List<Integer> match    = m.get(FieldType.MATCH);
		List<Integer> nonmatch = m.get(FieldType.NON_MATCH);
				
		assertEquals(expectedMatch,    match);
		assertEquals(expectedNonMatch, nonmatch);
	}
	
}
