package edu.mayo.pipes.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import com.tinkerpop.pipes.Pipe;

public class PipeTestUtils {

    public static  List<String> getResults(Pipe<String,String> pipe) {
    	List<String> results = new ArrayList<String>();
    	while(pipe.hasNext())
    		results.add(pipe.next());
    	return results;
    }
    
    public static void assertListsEqual(List<String> list1, List<String> list2) {
    	Assert.assertEquals("Array sizes are not equal!", list1.size(), list2.size());
    	for(int i=0; i < list1.size(); i++) {
    		Assert.assertEquals("Array item not equal!  Index: " + i, list1.get(i), list2.get(i));
    	}
    }

}
