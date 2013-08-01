package edu.mayo.pipes.history;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.mayo.pipes.util.metadata.Metadata;
import org.junit.Test;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.util.test.PipeTestUtils;

public class HistoryInPipeTest
{

	@Test
	public void test()
	{
		HistoryInPipe historyIn = new HistoryInPipe();

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
        
        Pipe<String, History> p = new Pipeline<String, History>(historyIn);

        p.setStarts(allRows);

        History history;
        
        // 1ST data line
        assertTrue(p.hasNext());
        history =  p.next();
        assertEquals(3, history.size());
        assertEquals("val1A", history.get(0));
        assertEquals("val1B", history.get(1));
        assertEquals("val1C", history.get(2));
        
        // 2ND data line
        assertTrue(p.hasNext());
        history =  p.next();
        assertEquals(3, history.size());
        assertEquals("val2A", history.get(0));
        assertEquals("val2B", history.get(1));
        assertEquals("val2C", history.get(2));

        // validate metadata
        HistoryMetaData meta = History.getMetaData();

        assertEquals("#COL_A,COL_B,COL_C", meta.getColumnHeaderRow(","));
        
        PipeTestUtils.assertListsEqual(headerRows, meta.getOriginalHeader());
        
        // check column metadata
        List<ColumnMetaData> cols = meta.getColumns();
        assertEquals(3, cols.size());
        assertEquals("COL_A", cols.get(0).getColumnName());
        assertEquals("COL_B", cols.get(1).getColumnName());
        assertEquals("COL_C", cols.get(2).getColumnName());
	}

	@Test
	public void testNoHeader()
	{
		HistoryInPipe historyIn = new HistoryInPipe();
		
        List<String> dataRows = Arrays.asList
        	(
       			"val1A\tval1B\tval1C",
       			"val2A\tval2B\tval2C"
        	);

        Pipe<String, History> p = new Pipeline<String, History>(historyIn);

        p.setStarts(dataRows);

        History history;
        
        // 1ST data line
        assertTrue(p.hasNext());
        history =  p.next();
        assertEquals(3, history.size());
        assertEquals("val1A", history.get(0));
        assertEquals("val1B", history.get(1));
        assertEquals("val1C", history.get(2));
        
        // 2ND data line
        assertTrue(p.hasNext());
        history =  p.next();
        assertEquals(3, history.size());
        assertEquals("val2A", history.get(0));
        assertEquals("val2B", history.get(1));
        assertEquals("val2C", history.get(2));

        // validate metadata
        HistoryMetaData meta = History.getMetaData();

        assertEquals("#UNKNOWN_1,#UNKNOWN_2,#UNKNOWN_3", meta.getColumnHeaderRow(","));
        
        assertEquals(0, meta.getOriginalHeader().size());
        
        // check column metadata
        List<ColumnMetaData> cols = meta.getColumns();
        assertEquals(3, cols.size());
        assertEquals("#UNKNOWN_1", cols.get(0).getColumnName());
        assertEquals("#UNKNOWN_2", cols.get(1).getColumnName());
        assertEquals("#UNKNOWN_3", cols.get(2).getColumnName());
	}	
	
	@Test
	public void testNoData()
	{
		HistoryInPipe historyIn = new HistoryInPipe();

        List<String> headerRows = Arrays.asList
        	(
       			"##header1",
       			"##header2",
       			"##header3",
       			"#COL_A\tCOL_B\tCOL_C"
        	);		
		        
        Pipe<String, History> p = new Pipeline<String, History>(historyIn);

        p.setStarts(headerRows);
        
        // no data lines
        assertFalse(p.hasNext());
        
        // validate metadata
        HistoryMetaData meta = History.getMetaData();

        assertEquals("#COL_A,COL_B,COL_C", meta.getColumnHeaderRow(","));
        
        PipeTestUtils.assertListsEqual(headerRows, meta.getOriginalHeader());
        
        // check column metadata
        List<ColumnMetaData> cols = meta.getColumns();
        assertEquals(3, cols.size());
        assertEquals("COL_A", cols.get(0).getColumnName());
        assertEquals("COL_B", cols.get(1).getColumnName());
        assertEquals("COL_C", cols.get(2).getColumnName());        
	}


    List<String> input = Arrays.asList
            (
                    "##header1",
                    "##header2",
                    "##header3",
                    "#COL_A\tCOL_B\tCOL_C",
                    "val1A\tval1B\tval1C",
                    "val2A\tval2B\tval2C"
            );

    @Test
    public void testMetadataToJSON(){
        Metadata md = new Metadata(Metadata.CmdType.ToTJson, "bior_vcf_to_tjson");
        HistoryInPipe historyIn = new HistoryInPipe(md);
        Pipe<String, History> p = new Pipeline<String, History>(historyIn);

        p.setStarts(input);

        History history;

        // 1ST data line
        assertTrue(p.hasNext());
        history =  p.next();
//        assertEquals(3, history.size());
//        assertEquals("val1A", history.get(0));
//        assertEquals("val1B", history.get(1));
//        assertEquals("val1C", history.get(2));
//
//        // 2ND data line
//        assertTrue(p.hasNext());
//        history =  p.next();
//        assertEquals(3, history.size());
//        assertEquals("val2A", history.get(0));
//        assertEquals("val2B", history.get(1));
//        assertEquals("val2C", history.get(2));
//
//        // validate metadata
//        HistoryMetaData meta = History.getMetaData();
//
//        assertEquals("#COL_A,COL_B,COL_C", meta.getColumnHeaderRow(","));
//
//        PipeTestUtils.assertListsEqual(headerRows, meta.getOriginalHeader());
//
//        // check column metadata
//        List<ColumnMetaData> cols = meta.getColumns();
//        assertEquals(3, cols.size());
//        assertEquals("COL_A", cols.get(0).getColumnName());
//        assertEquals("COL_B", cols.get(1).getColumnName());
//        assertEquals("COL_C", cols.get(2).getColumnName());

    }
}
