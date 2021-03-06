package edu.mayo.pipes.util.metadata;

import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.JSON.DrillPipe;
import edu.mayo.pipes.JSON.lookup.LookupPipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.history.HistoryOutPipe;
import edu.mayo.pipes.util.test.PipeTestUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: m102417
 * Date: 7/29/13
 * Time: 3:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class AddMetadataLinesTest {





    @Test
    public void testBuildHeaderLine(){
        System.out.println("TestBuildHeaderLine...");
        AddMetadataLines amdl = new AddMetadataLines();
        LinkedHashMap<String,String> attr = new LinkedHashMap<String,String>();
        attr.put("foo","A");
        attr.put("bar","B");
        attr.put("baz","C");
        assertEquals("##BIOR=<foo=\"A\",bar=\"B\",baz=\"C\">",amdl.buildHeaderLine(attr));
    }

    @Test
    public void testParseHeaderLine(){
        System.out.println("TestParseHeaderLine...");
        AddMetadataLines amdl = new AddMetadataLines();
        LinkedHashMap<String,String> hm = amdl.parseHeaderLine("##BIOR=<ID=\"bior.dbSNP137\",ShortUniqueName=\"dbSNP137\",Source=\"dbSNP\",Description=\"dbSNP version 137, Patch 10, Human\",Version=\"137\",Build=\"GRCh37.p10\">");
        assertEquals("bior.dbSNP137", hm.get("ID"));
        assertEquals("dbSNP137", hm.get("ShortUniqueName"));
        assertEquals("dbSNP version 137, Patch 10, Human",hm.get("Description"));
    }

    @Test
    public void testJsonArray(){
        System.out.println("TestJsonArray...");
        AddMetadataLines amdl = new AddMetadataLines();
        assertEquals("foo.bar.baz",amdl.fixArrayDrillPath("foo.bar.baz"));
        assertEquals("foo.bar.baz",amdl.fixArrayDrillPath("foo[0].bar.baz"));
        assertEquals("foo.bar.baz",amdl.fixArrayDrillPath("foo[*].bar.baz"));
        assertEquals("foo.bar.baz",amdl.fixArrayDrillPath("foo[*].bar[1].baz"));
        assertEquals("foo.bar.baz",amdl.fixArrayDrillPath("foo[*].bar[1].baz[3]"));
    }

    @Test
    public void testParseThenBuildHeaderLine() {
   	 	String IN = "##BIOR=<ID=\"bior.dbSNP137.INFO.SSR\","
   	 			+ "Operation=\"bior_drill\","
   	 			+ "DataType=\"String\","
   	 			+ "Key=\"INFO.SSR\","
   	 			+ "Delimiter=\",\","
   	 			+ "Description=\"Variant suspect reason code (0 - unspecified, 1 - paralog, 2 - byEST, 3 - Para_EST, 4 - oldAlign, 5 - other)\","
   	 			+ "CatalogShortUniqueName=\"dbSNP137\","
   	 			+ "CatalogSource=\"dbSNP\","
   	 			+ "CatalogVersion=\"137\","
   	 			+ "CatalogBuild=\"GRCh37.p10\","
   	 			+ "CatalogPath=\"/data5/bsi/catalogs/bior/v1/dbSNP/137/00-All-GRCh37.tsv.bgz\">";
   	 	AddMetadataLines adder = new AddMetadataLines();
   	 	String out = adder.buildHeaderLine(adder.parseHeaderLine(IN));
   	 	assertEquals(IN, out);
    }
    
    //this code should be dead after the changes to HistoryInPipe
    //    @Test
//    public void testConstructDrillLine() throws IOException {
//        History.clearMetaData();
//        System.out.println("TestConstructDrillLine...");
//        AddMetadataLines amdl = new AddMetadataLines();
//        Pipeline<String,History> pipe = new Pipeline(
//                new HistoryInPipe()
//        );
//        pipe.setStarts(output); //output of testConstructMetadataLine() is the input to drill
//        History h = pipe.next();
//
//        History hout = amdl.constructDrillLine(h, "bior.dbSNP137.INFO.SSR");
//
//        //there should now be 4 lines
//        assertEquals(4, History.getMetaData().getOriginalHeader().size());
//
//        for(int i=0; i< drillout.size()-1; i++){
//            assertEquals(drillout.get(i), History.getMetaData().getOriginalHeader().get(i));
//        }
//        StringBuilder sb = new StringBuilder();
//        for(String s : hout){
//            sb.append(s);
//            sb.append("\t");
//        }
//        assertEquals(drillout.get(drillout.size()-1)+"\t", sb.toString());
//
//    }
    //    @Test
//    public void testConstructMetadataLine() throws IOException {
//        History.clearMetaData();
//        System.out.println("Test ConstructMetadataLine...");
//        AddMetadataLines amdl = new AddMetadataLines();
//
//        Pipeline<String,History> pipe = new Pipeline(
//                new HistoryInPipe()
//        );
//        pipe.setStarts(input);
//        History h = pipe.next();
//
//        String[] name = LookupPipe.class.toString().split("\\.");
//        History hout = amdl.constructMetadataLine(h, "bior.dbSNP137", catalogFile, name[name.length-1]);//just an example
//
//        //check to make sure that the original data is there, but there is also a ##BIOR line
//        for (String s : History.getMetaData().getOriginalHeader()) {
//            System.out.println(s);
//        }
//
//        //there should now be 3 lines
//        assertEquals(3, History.getMetaData().getOriginalHeader().size());
//
//
//        for(int i=0; i< output.size()-1; i++){
//            assertEquals(output.get(i), History.getMetaData().getOriginalHeader().get(i));
//        }
//        StringBuilder sb = new StringBuilder();
//        for(String s : hout){
//            sb.append(s);
//            sb.append("\t");
//        }
//        assertEquals(output.get(output.size()-1)+"\t", sb.toString());
//
//
//    }
}
