package edu.mayo.pipes.util;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import edu.mayo.pipes.util.FieldSpecification;
import edu.mayo.pipes.util.FieldSpecification.FieldDirection;
import edu.mayo.pipes.util.FieldSpecification.FieldType;

public class FieldSpecificationTest
{
	
	@Test
	public void testNthOnlySingle()
	{
		List<Integer> match;
		List<Integer> nonmatch;
		String spec;
		FieldDirection direction;
		
		match     = Arrays.asList(5);
		nonmatch  = Arrays.asList(1, 2, 3, 4, 6);		
		spec      = "5";
		direction = FieldDirection.LEFT_TO_RIGHT;
		validate(spec, direction, match, nonmatch);
		
		match     = Arrays.asList(2);
		nonmatch  = Arrays.asList(1, 3, 4, 5, 6);		
		spec      = "5";
		direction = FieldDirection.RIGHT_TO_LEFT;
		validate(spec, direction, match, nonmatch);		
	}

	@Test
	public void testNthOnlyMultiple()
	{
		List<Integer> match;
		List<Integer> nonmatch;
		String spec;
		FieldDirection direction;

		match     = Arrays.asList(2, 5, 7);
		nonmatch  = Arrays.asList(1, 3, 4, 6);		
		spec      = "2,5,7";
		direction = FieldDirection.LEFT_TO_RIGHT;		
		validate(spec, direction, match, nonmatch);

		match     = Arrays.asList(1, 3, 6);
		nonmatch  = Arrays.asList(2, 4, 5, 7);		
		spec      = "2,5,7";
		direction = FieldDirection.RIGHT_TO_LEFT;		
		validate(spec, direction, match, nonmatch);
	}

	@Test
	public void testNthToEnd()
	{
		List<Integer> match;
		List<Integer> nonmatch;
		String spec;
		FieldDirection direction;

		match     = Arrays.asList(5, 6, 7);
		nonmatch  = Arrays.asList(1, 2, 3, 4);		
		spec      = "5-";
		direction = FieldDirection.LEFT_TO_RIGHT;		
		validate(spec, direction, match, nonmatch);
		
		match     = Arrays.asList(3, 4, 5, 6, 7);
		nonmatch  = Arrays.asList(1, 2);		
		spec      = "5-";
		direction = FieldDirection.RIGHT_TO_LEFT;		
		validate(spec, direction, match, nonmatch);		
	}	
	
	@Test
	public void testFirstToMth()
	{
		List<Integer> match;
		List<Integer> nonmatch;
		String spec;
		FieldDirection direction;

		match     = Arrays.asList(1, 2, 3);
		nonmatch  = Arrays.asList(4, 5, 6);		
		spec      = "-3";
		direction = FieldDirection.LEFT_TO_RIGHT;		
		validate(spec, direction, match, nonmatch);		
		
		match     = Arrays.asList(1, 2, 3, 4, 5);
		nonmatch  = Arrays.asList(6, 7);		
		spec      = "-3";
		direction = FieldDirection.RIGHT_TO_LEFT;		
		validate(spec, direction, match, nonmatch);			
	}	

	@Test
	public void testNthToMth()
	{
		List<Integer> match;
		List<Integer> nonmatch;
		String spec;
		FieldDirection direction;

		// 1 2 3 4 5 6 7
		// 7 6 5 4 3 2 1		
		
		match    = Arrays.asList(2, 3, 4, 5);
		nonmatch = Arrays.asList(1, 6, 7);		
		spec = "2-5";
		direction = FieldDirection.LEFT_TO_RIGHT;		
		validate(spec, direction, match, nonmatch);
		
		match    = Arrays.asList(3, 4, 5, 6);
		nonmatch = Arrays.asList(1, 2, 7);		
		spec = "2-5";
		direction = FieldDirection.RIGHT_TO_LEFT;	
		validate(spec, direction, match, nonmatch);		
	}	
	
	private void validate(String spec, FieldDirection direction, List<Integer> expectedMatch, List<Integer> expectedNonMatch)
	{
		FieldSpecification fSpec = new FieldSpecification(spec, direction);
		
		int numFields = expectedMatch.size() + expectedNonMatch.size();
		
		Map<FieldType, List<Integer>> m = fSpec.getFields(numFields);
		List<Integer> match    = m.get(FieldType.MATCH);
		List<Integer> nonmatch = m.get(FieldType.NON_MATCH);
				
		assertEquals(expectedMatch,    match);
		assertEquals(expectedNonMatch, nonmatch);
	}
	
}
