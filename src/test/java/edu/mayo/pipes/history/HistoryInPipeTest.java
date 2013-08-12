package edu.mayo.pipes.history;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.mayo.pipes.JSON.DrillPipe;
import edu.mayo.pipes.JSON.lookup.LookupPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.UNIX.GrepEPipe;
import edu.mayo.pipes.bioinformatics.VCF2VariantPipe;
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

    @Test
    public void testMetadataToJSON(){
        History.clearMetaData();
        Metadata md = new Metadata("bior_vcf_to_tjson");
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

    //note that for this example there is no metadata because it came (optionally) in the input
    public final List<String> jsondrill = Arrays.asList(
            "##fileformat=VCFv4.0",
            "##fileDate=20120616",
            "##source=dbSNP",
            "##dbSNP_BUILD_ID=137",
            "##reference=GRCh37.p5",
            "##phasing=partial",
            "##variationPropertyDocumentationUrl=ftp://ftp.ncbi.nlm.nih.gov/snp/specs/dbSNP_BitField_latest.pdf\t",
            "##FILTER=<ID=NC,Description=\"Inconsistent Genotype Submission For At Least One Sample\">",
            "##BIOR=<ID=\"bior.ToTJson\",Operation=\"bior_vcf_to_tjson\",DataType=\"JSON\",ShortUniqueName=\"ToTJson\">",
            "##BIOR=<ID=\"bior.ToTJson.INFO.RSPOS\",Operation=\"bior_drill\",DataType=\"STRING\",Field=\"INFO.RSPOS\",ShortUniqueName=\"ToTJson\">",
            "##BIOR=<ID=\"bior.ToTJson.INFO.SSR\",Operation=\"bior_drill\",DataType=\"STRING\",Field=\"INFO.SSR\",ShortUniqueName=\"ToTJson\">",
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tbior.ToTJson.INFO.RSPOS\tbior.ToTJson.INFO.SSR",
            "1\t10144\trs144773400\tTA\tT\t.\t.\tRSPOS=10145;dbSNPBuildID=134;SSR=0;SAO=0;VP=050000000005000002000200;WGT=1;VC=DIV;ASP;OTHERKG\t10145\t0",
            "1\t10177\trs201752861\tA\tC\t.\t.\tRSPOS=10177;dbSNPBuildID=137;SSR=0;SAO=0;VP=050000000005000002000100;WGT=1;VC=SNV;ASP;OTHERKG\t10177\t0",
            "1\t10180\trs201694901\tT\tC\t.\t.\tRSPOS=10180;dbSNPBuildID=137;SSR=0;SAO=0;VP=050000000005000002000100;WGT=1;VC=SNV;ASP;OTHERKG\t10180\t0",
            "1\t10228\trs143255646\tTA\tT\t.\t.\tRSPOS=10229;dbSNPBuildID=134;SSR=0;SAO=0;VP=050000000005000002000200;WGT=1;VC=DIV;ASP;OTHERKG\t10229\t0",
            "1\t10228\trs200462216\tTAACCCCTAACCCTAACCCTAAACCCTA\tT\t.\t.\tRSPOS=10229;dbSNPBuildID=137;SSR=0;SAO=0;VP=050000000005000002000200;WGT=1;VC=DIV;ASP;OTHERKG\t10229\t0",
            "1\t10231\trs200279319\tC\tA\t.\t.\tRSPOS=10231;dbSNPBuildID=137;SSR=0;SAO=0;VP=050000000005000002000100;WGT=1;VC=SNV;ASP;OTHERKG\t10231\t0",
            "1\t10234\trs145599635\tC\tT\t.\t.\tRSPOS=10234;dbSNPBuildID=134;SSR=0;SAO=0;VP=050000000005000002000100;WGT=1;VC=SNV;ASP;OTHERKG\t10234\t0"
    );

    /**
     * Test toJSON followed by a Drill
     * -- note, this also tess the case where there is no catalog
     */
    @Test
    public void testToJSONDrill(){
        History.clearMetaData();
        Metadata md = new Metadata("bior_vcf_to_tjson");
        String paths[] = new String[]{"INFO.RSPOS", "INFO.SSR"};
        Metadata mddrill = new Metadata(-1, "bior_drill", false, paths);
        ArrayList<Metadata> ops = new ArrayList<Metadata>();
        ops.add(md);
        ops.add(mddrill);
        HistoryInPipe historyIn = new HistoryInPipe(ops);
        Pipe<String, History> p = new Pipeline<String, History>(
                new CatPipe(),
                historyIn,
                new VCF2VariantPipe(),
                new DrillPipe(false, paths),
                new HistoryOutPipe(),
                new GrepEPipe("##INFO.*")
        );
        p.setStarts(Arrays.asList("src/test/resources/testData/example.vcf"));

        for(int i=0; jsondrill.size() > i; i++){
            assertEquals(jsondrill.get(i), p.next());
        }
    }


    public final List<String> toolout = Arrays.asList(
            "##header1",
            "##BIOR=<ID=\"bior.Vep\",Operation=\"bior_vep\",DataType=\"JSON\",ShortUniqueName=\"Vep\",Description=\"ENSEMBL VARIANT EFFECT PREDICTOR\",Version=\"2.7\",Build=\"GRCh37\",DataSourceProperties=\"src/test/resources/testData/metadata/vep.datasource.properties\",ColumnProperties=\"src/test/resources/testData/metadata/vep.columns.properties\">",
            "#COL_A\tCOL_B\tCOL_C\tbior.Vep",
            "val1A\tval1B\tval1C",
            "val2A\tval2B\tval2C"
    );

    @Test
    public void testMetadataTool(){
        History.clearMetaData();
        Metadata md = new Metadata("src/test/resources/testData/metadata/vep.datasource.properties", "src/test/resources/testData/metadata/vep.columns.properties", "bior_vep");
        HistoryInPipe historyIn = new HistoryInPipe(md);
        Pipe<String, History> p = new Pipeline<String, History>(historyIn, new HistoryOutPipe());
        p.setStarts(input);

        for(int i=0; p.hasNext(); i++){
            assertEquals(toolout.get(i), p.next());
        }

    }

    public final List<String> tooldrillout = Arrays.asList(
            "##header1",
            "##BIOR=<ID=\"bior.Vep\",Operation=\"bior_vep\",DataType=\"JSON\",ShortUniqueName=\"Vep\",Description=\"ENSEMBL VARIANT EFFECT PREDICTOR\",Version=\"2.7\",Build=\"GRCh37\",DataSourceProperties=\"src/test/resources/testData/metadata/vep.datasource.properties\",ColumnProperties=\"src/test/resources/testData/metadata/vep.columns.properties\">",
            "##BIOR=<ID=\"bior.Vep.FOO\",Operation=\"bior_drill\",DataType=\"STRING\",Field=\"FOO\",FieldDescription=\"\",ShortUniqueName=\"Vep\",Description=\"ENSEMBL VARIANT EFFECT PREDICTOR\",Version=\"2.7\",Build=\"GRCh37\",DataSourceProperties=\"src/test/resources/testData/metadata/vep.datasource.properties\",ColumnProperties=\"src/test/resources/testData/metadata/vep.columns.properties\">",
            "##BIOR=<ID=\"bior.Vep.BAR\",Operation=\"bior_drill\",DataType=\"STRING\",Field=\"BAR\",FieldDescription=\"\",ShortUniqueName=\"Vep\",Description=\"ENSEMBL VARIANT EFFECT PREDICTOR\",Version=\"2.7\",Build=\"GRCh37\",DataSourceProperties=\"src/test/resources/testData/metadata/vep.datasource.properties\",ColumnProperties=\"src/test/resources/testData/metadata/vep.columns.properties\">",
            "#COL_A\tCOL_B\tCOL_C\tbior.Vep.FOO\tbior.Vep.BAR\tbior.Vep",
            "val1A\tval1B\tval1C",
            "val2A\tval2B\tval2C"
    );

    /**
     * test inserting a tool followed by a drill  - just the metadata not the data rows
     */
    @Test
    public void testToolDrill(){
        History.clearMetaData();
        System.out.println("Test Tool Drill");
        String paths[] = new String[]{"FOO", "BAR"};
        Metadata mddrill = new Metadata(-1, "bior_drill", true, paths);
        HistoryInPipe hinDrill = new HistoryInPipe(mddrill);
        Pipe<String, History> p = new Pipeline<String, History>(hinDrill, new HistoryOutPipe());
        p.setStarts(toolout);

        for(int i=0; p.hasNext(); i++){
            //System.out.println(p.next());
            assertEquals(tooldrillout.get(i), p.next());
        }
        return;
    }


    public final List<String> querryout = Arrays.asList(
            "##header1",
            "##BIOR=<ID=\"bior.dbSNP137\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "#COL_A\tCOL_B\tCOL_C\tbior.dbSNP137",
            "val1A\tval1B\tval1C",
            "val2A\tval2B\tval2C"
    );

    //make sure that this is the full path when passed to this method in production!
    public final String catalogFile = "src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz";
    @Test
    public void testMetadataQuery(){
        History.clearMetaData();
        Metadata md = new Metadata(catalogFile, "bior_lookup");
        HistoryInPipe historyIn = new HistoryInPipe(md);
        Pipe<String, History> p = new Pipeline<String, History>(historyIn, new HistoryOutPipe());
        p.setStarts(input);

        for(int i=0; p.hasNext(); i++){
            assertEquals(querryout.get(i), p.next());
        }
    }

    //metadata no props
    public final List<String> noprops = Arrays.asList(
           "##header1",
            "##BIOR=<ID=\"bior.00-All_GRCh37\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"00-All_GRCh37\",Path=\"some/file/that/does/not/exist/00-All_GRCh37.tsv.bgz\">",
            "#COL_A\tCOL_B\tCOL_C\tbior.00-All_GRCh37",
            "val1A\tval1B\tval1C",
            "val2A\tval2B\tval2C"
    );

    @Test
    public void testMetadataQueryNoProps(){
        //same sort of test as above, but this case, we don't have a datasource.properties file for the catalog, so an exception will be caught, and we need to do an alternative...
        //this tests that alternative.
        History.clearMetaData();
        Metadata md = new Metadata("some/file/that/does/not/exist/00-All_GRCh37.tsv.bgz", "bior_lookup");  //note it is ok if there is no catalog for this test, because this just does the metadata, the lookup or whatever function will test the data processing.
        HistoryInPipe historyIn = new HistoryInPipe(md);
        Pipe<String, History> p = new Pipeline<String, History>(historyIn, new HistoryOutPipe());
        p.setStarts(input);

        for(int i=0; p.hasNext(); i++){
            assertEquals(noprops.get(i), p.next());
        }
    }

    //original file
    public final List<String> input2 = Arrays.asList(
            "##Header start",
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
            "1\t10144\trs144773400\tTA\tT\t.\t.\t.\t{\"Key\":\"Value\"}"
    );

    //some operation
    public final List<String> outputLookup = Arrays.asList(
            "##Header start",
            "##BIOR=<ID=\"bior.dbSNP137\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tbior.dbSNP137",
            "1\t10144\trs144773400\tTA\tT\t.\t.\t.\t{\"Key\":\"Value\"}"
    );

    //then the drill
    public final List<String> drillout = Arrays.asList(
            "##Header start",
            "##BIOR=<ID=\"bior.dbSNP137\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "##BIOR=<ID=\"bior.dbSNP137.INFO.SSR\",Operation=\"bior_drill\",DataType=\"STRING\",Field=\"INFO.SSR\",FieldDescription=\"Variant suspect reason code (0 - unspecified, 1 - paralog, 2 - byEST, 3 - Para_EST, 4 - oldAlign, 5 - other)\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tbior.dbSNP137.INFO.SSR\tbior.dbSNP137",
            "1\t10144\trs144773400\tTA\tT\t.\t.\t.\t{\"Key\":\"Value\"}" //note the absence of data, data is tested in Drill and other functions that deal with data
    );

    @Test
    public void testMetadataDrill(){
        //first do the lookup...
        History.clearMetaData();
        Metadata md = new Metadata(catalogFile, "bior_lookup");
        HistoryInPipe historyIn = new HistoryInPipe(md);
        Pipe<String, History> p = new Pipeline<String, History>(historyIn, new HistoryOutPipe());
        p.setStarts(input2);
        for(int i=0; p.hasNext(); i++){
            assertEquals(outputLookup.get(i), p.next());
        }
        //followed by the drill...
        History.clearMetaData();
        String paths[] = new String[]{"INFO.SSR"};
        Metadata mddrill = new Metadata(-1, "bior_drill", true, paths);
        HistoryInPipe hinDrill = new HistoryInPipe(mddrill);
        Pipe<String, History> p2 = new Pipeline<String, History>(hinDrill, new HistoryOutPipe());
        p2.setStarts(outputLookup);
        for(int i=0; p2.hasNext(); i++){
            assertEquals(drillout.get(i), p2.next());
        }

    }

    public final List<String> multidrillout = Arrays.asList(
            "##Header start",
            "##BIOR=<ID=\"bior.dbSNP137\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "##BIOR=<ID=\"bior.dbSNP137.INFO.SSR\",Operation=\"bior_drill\",DataType=\"STRING\",Field=\"INFO.SSR\",FieldDescription=\"Variant suspect reason code (0 - unspecified, 1 - paralog, 2 - byEST, 3 - Para_EST, 4 - oldAlign, 5 - other)\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "##BIOR=<ID=\"bior.dbSNP137.INFO.VC\",Operation=\"bior_drill\",DataType=\"STRING\",Field=\"INFO.VC\",FieldDescription=\"Variation Class\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tbior.dbSNP137.INFO.SSR\tbior.dbSNP137.INFO.VC\tbior.dbSNP137",
            "1\t10144\trs144773400\tTA\tT\t.\t.\t.\t{\"Key\":\"Value\"}" //note the absence of data, data is tested in Drill and other functions that deal with data
    );

    //test multiple drill paths
    @Test
    public void testMultiDrill(){
        History.clearMetaData();
        String paths[] = new String[]{"INFO.SSR", "INFO.VC"};
        Metadata mddrill = new Metadata(-1, "bior_drill", true, paths);
        HistoryInPipe hinDrill = new HistoryInPipe(mddrill);
        Pipe<String, History> p2 = new Pipeline<String, History>(hinDrill, new HistoryOutPipe());
        p2.setStarts(outputLookup);
        for(int i=0; p2.hasNext(); i++){
            assertEquals(multidrillout.get(i), p2.next());
        }
    }

    public final List<String> multidrilloutdashk = Arrays.asList(
            "##Header start",
            "##BIOR=<ID=\"bior.dbSNP137\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "##BIOR=<ID=\"bior.dbSNP137.INFO.SSR\",Operation=\"bior_drill\",DataType=\"STRING\",Field=\"INFO.SSR\",FieldDescription=\"Variant suspect reason code (0 - unspecified, 1 - paralog, 2 - byEST, 3 - Para_EST, 4 - oldAlign, 5 - other)\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "##BIOR=<ID=\"bior.dbSNP137.INFO.VC\",Operation=\"bior_drill\",DataType=\"STRING\",Field=\"INFO.VC\",FieldDescription=\"Variation Class\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tbior.dbSNP137.INFO.SSR\tbior.dbSNP137.INFO.VC",
            "1\t10144\trs144773400\tTA\tT\t.\t.\t.\t{\"Key\":\"Value\"}" //note the absence of data, data is tested in Drill and other functions that deal with data
    );

    //test for drill where -k is removing the json
    @Test
    public void testKeepFalseDrill(){
        History.clearMetaData();
        String paths[] = new String[]{"INFO.SSR", "INFO.VC"};
        Metadata mddrill = new Metadata(-1, "bior_drill", false, paths);
        HistoryInPipe hinDrill = new HistoryInPipe(mddrill);
        Pipe<String, String> p2 = new Pipeline<String, String>(hinDrill, new HistoryOutPipe());
        p2.setStarts(outputLookup);
        for(int i=0; p2.hasNext(); i++){
            String result = p2.next();
            assertEquals(multidrilloutdashk.get(i), result);
        }
    }

    public final List<String> drillnoprops = Arrays.asList(
            "##header1",
            "##BIOR=<ID=\"bior.00-All_GRCh37\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"00-All_GRCh37\",Path=\"some/file/that/does/not/exist/00-All_GRCh37.tsv.bgz\">",
            "##BIOR=<ID=\"bior.00-All_GRCh37.INFO.SSR\",Operation=\"bior_drill\",DataType=\"STRING\",Field=\"INFO.SSR\",FieldDescription=\"\",ShortUniqueName=\"00-All_GRCh37\",Path=\"some/file/that/does/not/exist/00-All_GRCh37.tsv.bgz\">",
            "#COL_A\tCOL_B\tCOL_C\tbior.00-All_GRCh37.INFO.SSR",
            "val1A\tval1B\tval1C",
            "val2A\tval2B\tval2C"
    );

    //add test for if the drill does not have property files for the catalog
    @Test
    public void testNoPropsDrill(){
        History.clearMetaData();
        String paths[] = new String[]{"INFO.SSR"};
        Metadata mddrill = new Metadata(-1, "bior_drill", false, paths);
        HistoryInPipe hinDrill = new HistoryInPipe(mddrill);
        Pipe<String, String> p2 = new Pipeline<String, String>(hinDrill, new HistoryOutPipe());
        p2.setStarts(noprops);
        for(int i=0; p2.hasNext(); i++){
            String result = p2.next();
            assertEquals(drillnoprops.get(i), result);
        }
    }

    public final List<String> querryagain = Arrays.asList(
            "##header1",
            "##BIOR=<ID=\"bior.dbSNP137\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "##BIOR=<ID=\"bior.dbSNP137.2\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "#COL_A\tCOL_B\tCOL_C\tbior.dbSNP137\tbior.dbSNP137.2",
            "val1A\tval1B\tval1C",
            "val2A\tval2B\tval2C"
    );

    //same datasource same version
    @Test
    public void testQuerySameDataSourceAgain(){
        History.clearMetaData();
        String paths[] = new String[]{"INFO.SSR"};
        Metadata md = new Metadata(catalogFile, "bior_lookup");
        HistoryInPipe hinDrill = new HistoryInPipe(md);
        Pipe<String, String> p2 = new Pipeline<String, String>(hinDrill, new HistoryOutPipe());
        p2.setStarts(querryout);
        for(int i=0; p2.hasNext(); i++){
            String result = p2.next();
            assertEquals(querryagain.get(i), result);
        }
    }

    public final List<String> drillarrayout = Arrays.asList(
            "##Header start",
            "##BIOR=<ID=\"bior.dbSNP137\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "##BIOR=<ID=\"bior.dbSNP137._altAlleles[0]\",Operation=\"bior_drill\",DataType=\"STRING\",Field=\"_altAlleles\",FieldDescription=\"BioR required field - alternate alleles (from ALT field) - JSON array\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tbior.dbSNP137._altAlleles[0]\tbior.dbSNP137",
            "1\t10144\trs144773400\tTA\tT\t.\t.\t.\t{\"Key\":\"Value\"}" //note the absence of data, data is tested in Drill and other functions that deal with data
    );

    //test case for json array
    @Test
    public void testJsonArrayMetadata(){
        History.clearMetaData();
        String paths[] = new String[]{"_altAlleles[0]"};
        Metadata mddrill = new Metadata(-1, "bior_drill", true, paths);
        HistoryInPipe hinDrill = new HistoryInPipe(mddrill);
        Pipe<String, History> p2 = new Pipeline<String, History>(hinDrill, new HistoryOutPipe());
        p2.setStarts(outputLookup);
        for(int i=0; p2.hasNext(); i++){
            assertEquals(drillarrayout.get(i), p2.next());
        }

    }


    public final List<String> querryouterr = Arrays.asList(
            "##header1",
            "##BIOR=<ID=\"bior.dbSNP137\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"\",Build=\"\",Path=\"src/test/resources/testData/metadata/fieldsNotFilledIn/00-All_GRCh37.tsv.bgz\">",
            "#COL_A\tCOL_B\tCOL_C\tbior.dbSNP137",
            "val1A\tval1B\tval1C",
            "val2A\tval2B\tval2C"
    );


    //test cases for when properties are not defined.
    public final String catalogMessedUpFile = "src/test/resources/testData/metadata/fieldsNotFilledIn/00-All_GRCh37.tsv.bgz";
    @Test
    public void testDatasourcePropertiesNotComplete(){
        History.clearMetaData();
        Metadata md = new Metadata(catalogMessedUpFile, "bior_lookup");
        HistoryInPipe historyIn = new HistoryInPipe(md);
        Pipe<String, History> p = new Pipeline<String, History>(historyIn, new HistoryOutPipe());
        p.setStarts(input);

        for(int i=0; p.hasNext(); i++){
            assertEquals(querryouterr.get(i), p.next());
        }
    }

    //then the drill
    public final List<String> drillmessedout = Arrays.asList(
            "##header1",
            "##BIOR=<ID=\"bior.dbSNP137\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"\",Build=\"\",Path=\"src/test/resources/testData/metadata/fieldsNotFilledIn/00-All_GRCh37.tsv.bgz\">",
            "##BIOR=<ID=\"bior.dbSNP137.INFO.SSR\",Operation=\"bior_drill\",DataType=\"STRING\",Field=\"INFO.SSR\",FieldDescription=\"\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"\",Build=\"\",Path=\"src/test/resources/testData/metadata/fieldsNotFilledIn/00-All_GRCh37.tsv.bgz\">",
            "#COL_A\tCOL_B\tCOL_C\tbior.dbSNP137.INFO.SSR\tbior.dbSNP137",
            "val1A\tval1B\tval1C",
            "val2A\tval2B\tval2C" //note the absence of data, data is tested in Drill and other functions that deal with data
    );

    @Test
    public void testMetadataDrillMessedProps(){
        History.clearMetaData();
        String paths[] = new String[]{"INFO.SSR"};
        Metadata mddrill = new Metadata(-1, "bior_drill", true, paths);
        HistoryInPipe hinDrill = new HistoryInPipe(mddrill);
        Pipe<String, History> p2 = new Pipeline<String, History>(hinDrill, new HistoryOutPipe());
        p2.setStarts(querryouterr);
        for(int i=0; p2.hasNext(); i++){
            assertEquals(drillmessedout.get(i), p2.next());
        }

    }

    public final List<String> escapequotes = Arrays.asList(
            "##header1",
            "##BIOR=<ID=\"bior.dbSNP137\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "##BIOR=<ID=\"bior.dbSNP137.INFO.VP\",Operation=\"bior_drill\",DataType=\"STRING\",Field=\"INFO.VP\",FieldDescription=\"Variation <q>Property.<q>  Documentation is at ftp://ftp.ncbi.nlm.nih.gov/snp/specs/dbSNP_BitField_latest.pdf\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\",Path=\"src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz\">",
            "#COL_A\tCOL_B\tCOL_C\tbior.dbSNP137.INFO.VP\tbior.dbSNP137",
            "val1A\tval1B\tval1C",
            "val2A\tval2B\tval2C" //note the absence of data, data is tested in Drill and other functions that deal with data
    );

    @Test
    public void testEscapeQuotes(){
        History.clearMetaData();
        String paths[] = new String[]{"INFO.VP"};
        Metadata mddrill = new Metadata(-1, "bior_drill", true, paths);
        HistoryInPipe hinDrill = new HistoryInPipe(mddrill);
        Pipe<String, History> p2 = new Pipeline<String, History>(hinDrill, new HistoryOutPipe());
        p2.setStarts(querryout);
        for(int i=0; p2.hasNext(); i++){
            assertEquals(escapequotes.get(i), p2.next());
        }
    }



}
