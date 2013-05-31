package edu.mayo.pipes.history;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

public class HistoryOutPipeTest {

	@Test
	public void test()
	{
		HistoryInPipe  historyIn  = new HistoryInPipe();
		HistoryOutPipe historyOut = new HistoryOutPipe();

        List<String> headerRows = Arrays.asList
        	(
       			"##header1",
       			"##header2",
       			"##header3",
       			"#COL_A\tCOL_B\tCOL_C"
        	);		
		
        List<String> dataRows = Arrays.asList
        	(
       			"val1A\tval1B\tval1C",
       			"val2A\tval2B\tval2C"
        	);

        List<String> allRows = new ArrayList<String>();
        allRows.addAll(headerRows);
        allRows.addAll(dataRows);
        
        Pipe<String, String> p = new Pipeline<String, String>(historyIn, historyOut);

        p.setStarts(allRows);

        for (String row: allRows)
        {
            assertEquals(p.next(), row);        	
        }        
	}

}
