package edu.mayo.pipes.util.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import com.tinkerpop.pipes.Pipe;

import edu.mayo.pipes.history.History;


/** Put this class in the main java packages so it can be picked up an used by other projects 
 *  such as bior_pipeline and bior_catalog  */
public class PipeTestUtils {

    public static  List<String> getResults(Pipe<String,String> pipe) {
    	List<String> results = new ArrayList<String>();
    	while(pipe.hasNext())
    		results.add(pipe.next());
    	return results;
    }
    
	public static ArrayList<String> pipeOutputToStrings(Pipe<History, History> pipe) {
		ArrayList<String> lines = new ArrayList<String>();
		while(pipe.hasNext()) {
			History history = pipe.next();
			lines.add(history.getMergedData("\t"));
		}
		return lines;
	}

	public static ArrayList<String> pipeOutputToStrings2(Pipe<Object, String> pipe) {
		ArrayList<String> lines = new ArrayList<String>();
		while(pipe.hasNext()) {
			String str = pipe.next();
			lines.add(str);
		}
		return lines;
	}

    
    public static void assertListsEqual(List<String> expected, List<String> actual) {
    	Assert.assertEquals("Array sizes are not equal!", expected.size(), actual.size());
    	for(int i=0; i < expected.size(); i++) {
    		Assert.assertEquals("Array item not equal!  Line: " + (i+1), expected.get(i), actual.get(i));
    	}
    }

}
