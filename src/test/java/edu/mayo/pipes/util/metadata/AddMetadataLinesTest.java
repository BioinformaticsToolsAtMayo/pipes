package edu.mayo.pipes.util.metadata;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
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
        AddMetadataLines amdl = new AddMetadataLines();
        List<String> attr = new ArrayList<String>();
        attr.add("foo");
        attr.add("bar");
        attr.add("baz");
        assertEquals("##BIOR=<foo,bar,baz>",amdl.buildHeaderLine(attr));
    }
}
