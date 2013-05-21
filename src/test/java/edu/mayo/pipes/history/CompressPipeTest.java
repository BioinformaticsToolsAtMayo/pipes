package edu.mayo.pipes.history;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.util.FieldSpecification;
import edu.mayo.pipes.util.FieldSpecification.FieldDirection;


public class CompressPipeTest {

    @Test
    public void testProcessNextStart() throws IOException, InterruptedException {
        
    	String delimiter = "|";
        FieldSpecification fieldSpec = new FieldSpecification("2,3");
        CompressPipe compress = new CompressPipe(fieldSpec, delimiter);
        
        List<List<String>> asList = Arrays.asList
        	(
        		Arrays.asList("dataA", "1", "A"),
        		Arrays.asList("dataA", "2", "B"),
        		Arrays.asList("dataA", "3", "C"),
        		Arrays.asList("dataB", "100", "W"),
        		Arrays.asList("dataB", "101", "X"),
        		Arrays.asList("dataC", "333", "Z")        		
        	);

        Pipe<List<String>, List<String>> p = new Pipeline<List<String>, List<String>>(compress);

        p.setStarts(asList);

        List<String> line;
        
        // 1ST compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataA", "1|2|3", "A|B|C"), line);
        
        // 2ND compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataB", "100|101", "W|X"), line);

        // 3RD compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataC", "333", "Z"), line);
    }

    @Test
    public void testRightToLeft() throws IOException, InterruptedException {
        
    	String delimiter = "|";
        FieldSpecification fieldSpec = new FieldSpecification("2,3", FieldDirection.RIGHT_TO_LEFT);
        CompressPipe compress = new CompressPipe(fieldSpec, delimiter);
        
        List<List<String>> asList = Arrays.asList
        	(
        		Arrays.asList("dataX", "1", "A"),
        		Arrays.asList("dataY", "2", "A"),
        		Arrays.asList("dataA", "3", "C"),
        		Arrays.asList("dataB", "100", "W"),
        		Arrays.asList("dataB", "101", "Z"),
        		Arrays.asList("dataC", "333", "Z")        		
        	);

        Pipe<List<String>, List<String>> p = new Pipeline<List<String>, List<String>>(compress);

        p.setStarts(asList);

        List<String> line;
        
        // 1ST compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataX|dataY", "1|2", "A"), line);
        
        // 2ND compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataA", "3", "C"), line);

        // 3RD compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataB", "100", "W"), line);

        // 4TH compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataB|dataC", "101|333", "Z"), line);
    }    
    
    @Test
    public void testDelimiterConflictDefault() throws IOException, InterruptedException {
        
    	String delimiter = "|";
        FieldSpecification fieldSpec = new FieldSpecification("2");
        CompressPipe compress = new CompressPipe(fieldSpec, delimiter);
        
        List<List<String>> asList = Arrays.asList
        	(
        		Arrays.asList("dataA", "1|A"),
        		Arrays.asList("dataA", "2|B")
        	);

        Pipe<List<String>, List<String>> p = new Pipeline<List<String>, List<String>>(compress);

        p.setStarts(asList);
        
        // compressed line
        assertTrue(p.hasNext());
        List<String> line = (List<String>) p.next();
        validate(Arrays.asList("dataA", "1\\|A|2\\|B"), line);        
    }        
    
    @Test
    public void testDelimiterConflict() throws IOException, InterruptedException {
        
    	String delimiter = "|";
    	String escDelimiter = "%%";
        FieldSpecification fieldSpec = new FieldSpecification("2");
        CompressPipe compress = new CompressPipe(fieldSpec, delimiter, escDelimiter);
        
        List<List<String>> asList = Arrays.asList
        	(
        		Arrays.asList("dataA", "1|A"),
        		Arrays.asList("dataA", "2|B")
        	);

        Pipe<List<String>, List<String>> p = new Pipeline<List<String>, List<String>>(compress);

        p.setStarts(asList);
        
        // compressed line
        assertTrue(p.hasNext());
        List<String> line = (List<String>) p.next();
        validate(Arrays.asList("dataA", "1%%A|2%%B"), line);        
    }    
    
    @Test
    public void testDuplicates()
    {
    	String delimiter = "|";
        FieldSpecification fieldSpec = new FieldSpecification("2,3");
        CompressPipe compress = new CompressPipe(fieldSpec, delimiter);
        
        List<List<String>> asList = Arrays.asList
        	(
        		Arrays.asList("dataA", "foo", "A"),
        		Arrays.asList("dataA", "foo", "B"),
        		Arrays.asList("dataA", "foo", "C"),
        		Arrays.asList("dataB", "100", "bar"),
        		Arrays.asList("dataB", "101", "bar"),
        		Arrays.asList("dataB", "333", "bar"),        		
        		Arrays.asList("dataC", "foo", "bar"),
        		Arrays.asList("dataC", "foo", "bar"),
        		Arrays.asList("dataC", "foo", "bar")        		
        	);

        Pipe<List<String>, List<String>> p = new Pipeline<List<String>, List<String>>(compress);

        p.setStarts(asList);

        List<String> line;
        
        // 1ST compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataA", "foo", "A|B|C"), line);
        
        // 2ND compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataB", "100|101|333", "bar"), line);

        // 3RD compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataC", "foo", "bar"), line);
    	
    }
    
    private void validate(List<String> list1, List<String> list2)
    {
    	assertEquals(list1.size(), list2.size());
    	for (int i=0; i < list1.size(); i++)
    	{
    		assertEquals(list1.get(i), list2.get(i));
    	}
    }
}
