package edu.mayo.pipes.history;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.util.FieldSpecification;
import edu.mayo.pipes.util.FieldSpecification.FieldDirection;
import edu.mayo.pipes.util.test.PipeTestUtils;


public class CompressPipeTest {

	@Before
	public void beforeEach() {
		History.clearMetaData();
	}
	
	@After
	public void afterEach() {
		History.clearMetaData();
	}
	
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
        		Arrays.asList("dataC", "333", "."),        		
        		Arrays.asList("dataC", "334", "."),        		
        		Arrays.asList("dataD", "555", "Z")        		
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
        validate(Arrays.asList("dataC", "333|334", ".|."), line);

        // 4TH compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataD", "555", "Z"), line);
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
        		Arrays.asList("dataA", "2|B"),
        		Arrays.asList("dataB", "3|Z")
        	);

        Pipe<List<String>, List<String>> p = new Pipeline<List<String>, List<String>>(compress);

        p.setStarts(asList);
        
        // compressed line
        assertTrue(p.hasNext());
        List<String> line = (List<String>) p.next();
        validate(Arrays.asList("dataA", "1\\|A|2\\|B"), line);
        
        // compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataB", "3\\|Z"), line);        
    }        
    
    @Test
    public void testDelimiterConflict() throws IOException, InterruptedException {
        
    	String delimiter = "|";
    	String escDelimiter = "%%";
        FieldSpecification fieldSpec = new FieldSpecification("2");
        CompressPipe compress = new CompressPipe(fieldSpec, delimiter, escDelimiter, false);
        
        List<List<String>> asList = Arrays.asList
        	(
            		Arrays.asList("dataA", "1|A"),
            		Arrays.asList("dataA", "2|B"),
            		Arrays.asList("dataB", "3|Z")
        	);

        Pipe<List<String>, List<String>> p = new Pipeline<List<String>, List<String>>(compress);

        p.setStarts(asList);
        
        // compressed line
        assertTrue(p.hasNext());
        List<String> line = (List<String>) p.next();
        validate(Arrays.asList("dataA", "1%%A|2%%B"), line);        

        // compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataB", "3%%Z"), line);        
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
        validate(Arrays.asList("dataA", "foo|foo|foo", "A|B|C"), line);
        
        // 2ND compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataB", "100|101|333", "bar|bar|bar"), line);

        // 3RD compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataC", "foo|foo|foo", "bar|bar|bar"), line);
    	
    }

    @Test
    public void testSetLogic()
    {
    	String delimiter = "|";
        FieldSpecification fieldSpec = new FieldSpecification("2,3");
    	String escDelimiter = "%%";
    	boolean useSetCompression = true;
        CompressPipe compress = new CompressPipe(fieldSpec, delimiter, escDelimiter, useSetCompression);
        
        List<List<String>> asList = Arrays.asList
        	(
        		Arrays.asList("dataA", "foo", "A"),
        		Arrays.asList("dataA", "oof", "B"),
        		Arrays.asList("dataA", "foo", "C"),
        		Arrays.asList("dataB", "100", "bar"),
        		Arrays.asList("dataB", "101", "rab"),
        		Arrays.asList("dataB", "333", "bar"),        		
        		Arrays.asList("dataB", "000", "abc"),        		
        		Arrays.asList("dataC", ".", "."),
        		Arrays.asList("dataC", ".", "abc"),
        		Arrays.asList("dataC", ".", ".")        		
        	);

        Pipe<List<String>, List<String>> p = new Pipeline<List<String>, List<String>>(compress);

        p.setStarts(asList);

        List<String> line;
        
        // 1ST compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataA", "foo|oof", "A|B|C"), line);
        
        // 2ND compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataB", "100|101|333|000", "bar|rab|abc"), line);

        // 3RD compressed line
        assertTrue(p.hasNext());
        line = (List<String>) p.next();
        validate(Arrays.asList("dataC", ".", "abc"), line);
    	
    }    

    @Test
    public void testDuplicatesWithSetLogic()
    {
    	String delimiter = "|";
        FieldSpecification fieldSpec = new FieldSpecification("2,3");
    	String escDelimiter = "%%";
    	boolean useSetCompression = true;        
        CompressPipe compress = new CompressPipe(fieldSpec, delimiter, escDelimiter, useSetCompression);
        
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
    
    @Test
    public void testWithHistory()
    {
    	try {
	        CompressPipe compressPipe = new CompressPipe(new FieldSpecification("2,3"), "|", "%%", true);
	
			Pipeline pipeline = new Pipeline(
					new HistoryInPipe(compressPipe.getMetadata()),
					compressPipe,
					new HistoryOutPipe()
					);
	
	        List<String> in = Arrays.asList(
	        	"##BIOR=<ID=\"Data\",Number=\"1\",DataType=\"String\">",
	        	"##BIOR=<ID=\"Foobar\",Number=\"1\",DataType=\"String\">",
	        	"##BIOR=<ID=\"Letters\",Number=\"1\",DataType=\"String\">",
	        	"##BIOR=<ID=\"MyNumbers\",Number=\"1\",DataType=\"Integer\">",
	        	"#Data\tFoobar\tLetters\tMyNumbers",
	        	"dataA\tfoo\tA\t0",
	        	"dataA\tfoo\tB\t0",
	        	"dataA\tfoo\tC\t2",
	        	"dataB\t100\tbar\t3",
	        	"dataB\t101\tbar\t3",
	        	"dataB\t333\tbar\t3",        		
	        	"dataC\tfoo\tbar\t7",
	        	"dataC\tfoo\tbar\t7",
	        	"dataC\tfoo\tbar\t100"        		
	        	);
	
	        pipeline.setStarts(in);
	
	        List<String> actual = PipeTestUtils.getResults(pipeline);
	        List<String> expected = Arrays.asList(
	        		"##BIOR=<ID=\"Data\",Number=\"1\",DataType=\"String\">",
	        		"##BIOR=<ID=\"Foobar\",Number=\".\",DataType=\"String\",Delimiter=\"|\",EscapedDelimiter=\"%%\">",
	        		"##BIOR=<ID=\"Letters\",Number=\".\",DataType=\"String\",Delimiter=\"|\",EscapedDelimiter=\"%%\">",
	        		"##BIOR=<ID=\"MyNumbers\",Number=\"1\",DataType=\"Integer\">",
		        	"#Data\tFoobar\tLetters\tMyNumbers",
	                "dataA\tfoo\tA|B\t0",
	                "dataA\tfoo\tC\t2",
	                "dataB\t100|101|333\tbar\t3",
	                "dataC\tfoo\tbar\t7",
	                "dataC\tfoo\tbar\t100"
	        		);
	        PipeTestUtils.assertListsEqual(expected, actual);
    	}catch(Exception e) {
    		e.printStackTrace();
    	}
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
