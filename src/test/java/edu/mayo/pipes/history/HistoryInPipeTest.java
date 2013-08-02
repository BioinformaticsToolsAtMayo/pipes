package edu.mayo.pipes.history;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.mayo.pipes.JSON.lookup.LookupPipe;
import edu.mayo.pipes.util.metadata.AddMetadataLines;
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
        History.clearMetaData();
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
        History.clearMetaData();
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
        History.clearMetaData();
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
                    "#COL_A\tCOL_B\tCOL_C",
                    "val1A\tval1B\tval1C",
                    "val2A\tval2B\tval2C"
            );

    //@Test
    public void testMetadataToJSON(){
        History.clearMetaData();
        Metadata md = new Metadata(Metadata.CmdType.ToTJson, "bior_vcf_to_tjson");
        HistoryInPipe historyIn = new HistoryInPipe(md);
        Pipe<String, History> p = new Pipeline<String, History>(historyIn, new HistoryOutPipe());
        p.setStarts(input);

        assertEquals("##header1",
                p.next());
        assertEquals("##BIOR=<ID=\"bior.ToTJson\",Operation=\"bior_vcf_to_tjson\",DataType=\"JSON\",ShortUniqueName=\"ToTJson\">",
                p.next());
        assertEquals("#COL_A\tCOL_B\tCOL_C\tbior.ToTJson",
                p.next());
        assertEquals("val1A\tval1B\tval1C",
                p.next());
    }

    //@Test
    public void testMetadataTool(){
        History.clearMetaData();
        Metadata md = new Metadata(Metadata.CmdType.Tool, "src/test/resources/testData/metadata/vep.datasource.properties", "bior_vep");
        HistoryInPipe historyIn = new HistoryInPipe(md);
        Pipe<String, History> p = new Pipeline<String, History>(historyIn, new HistoryOutPipe());
        p.setStarts(input);

        assertEquals("##header1",
                p.next());
        assertEquals("##BIOR=<ID=\"bior.Vep\",Operation=\"bior_vep\",DataType=\"JSON\",ShortUniqueName=\"Vep\",Description=\"ENSEMBL VARIANT EFFECT PREDICTOR\",Version=\"2.7\",Build=\"GRCh37\">",
                p.next());
        assertEquals("#COL_A\tCOL_B\tCOL_C\tbior.Vep",
                p.next());
        assertEquals("val1A\tval1B\tval1C",
                p.next());
    }

    //write a test case for a second column with the same datasource different version
    //same datasource same version


    public final List<String> input2 = Arrays.asList(
            "##Header start",
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tbior.dbSNP137",
            "1\t10144\trs144773400\tTA\tT\t.\t.\t.\t{\"Key\":\"Value\"}"
    );

    public final List<String> output = Arrays.asList(
            "##Header start",
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tbior.dbSNP137",
            "##BIOR=<ID=\"bior.dbSNP137\",Operation=\"LookupPipe\",DataType=\"JSON\",CatalogShortUniqueName=\"dbSNP137\",CatalogSource=\"dbSNP\",CatalogVersion=\"137\",CatalogBuild=\"GRCh37.p10\",CatalogPath=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "1\t10144\trs144773400\tTA\tT\t.\t.\t.\t{\"Key\":\"Value\"}"
    );

    public final List<String> drillout = Arrays.asList(
            "##Header start",
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tbior.dbSNP137",
            "##BIOR=<ID=\"bior.dbSNP137\",Operation=\"LookupPipe\",DataType=\"JSON\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP from NCBI\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "##BIOR=<ID=\"bior.dbSNP137.INFO.SSR\",Operation=\"DrillPipe\",DataType=\"STRING\",Field=\"INFO.SSR\",FieldDescription=\"Variant suspect reason code (0 - unspecified, 1 - paralog, 2 - byEST, 3 - Para_EST, 4 - oldAlign, 5 - other)\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP from NCBI\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">\n",
            "1\t10144\trs144773400\tTA\tT\t.\t.\t.\t{\"Key\":\"Value\"}"
    );


    //make sure that this is the full path when passed to this method in production!
    public final String catalogFile = "src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz";
    //@Test
    public void testMetadataQuery(){
        History.clearMetaData();
        Metadata md = new Metadata(Metadata.CmdType.Query, catalogFile, "bior_lookup");
        HistoryInPipe historyIn = new HistoryInPipe(md);
        Pipe<String, History> p = new Pipeline<String, History>(historyIn, new HistoryOutPipe());
        p.setStarts(input);

        assertEquals("##header1",
                p.next());
        assertEquals("##BIOR=<ID=\"bior.dbSNP137\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
                p.next());
        assertEquals("#COL_A\tCOL_B\tCOL_C\tbior.dbSNP137",
                p.next());
        assertEquals("val1A\tval1B\tval1C",
                p.next());
    }

    //@Test
    public void testMetadataQueryNoProps(){
        //same sort of test as above, but this case, we don't have a datasource.properties file for the catalog, so an exception will be caught, and we need to do an alternative...
        //this tests that alternative.
        History.clearMetaData();
        Metadata md = new Metadata(Metadata.CmdType.Query, "some/file/that/does/not/exist/00-All_GRCh37.tsv.bgz", "bior_lookup");  //note it is ok if there is no catalog for this test, because this just does the metadata, the lookup or whatever function will test the data processing.
        HistoryInPipe historyIn = new HistoryInPipe(md);
        Pipe<String, History> p = new Pipeline<String, History>(historyIn, new HistoryOutPipe());
        p.setStarts(input);

        assertEquals("##header1",
                p.next());
        assertEquals("##BIOR=<ID=\"bior.00-All_GRCh37\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"00-All_GRCh37\",Path=\"some/file/that/does/not/exist/00-All_GRCh37.tsv.bgz\">",
                p.next());
        assertEquals("#COL_A\tCOL_B\tCOL_C\tbior.00-All_GRCh37",
                p.next());
        assertEquals("val1A\tval1B\tval1C",
                p.next());

    }


}
