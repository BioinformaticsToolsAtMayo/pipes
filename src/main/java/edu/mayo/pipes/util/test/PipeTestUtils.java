package edu.mayo.pipes.util.test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

import edu.mayo.pipes.history.History;


/** Put this class in the main java packages so it can be picked up an used by other projects 
 *  such as bior_pipeline and bior_catalog  */
public class PipeTestUtils {

	public static List<String> getResults(Pipe pipe) {
    	List<String> results = new ArrayList<String>();
    	while(pipe.hasNext()) {
    		Object obj = pipe.next();
    		//System.out.println("Object type: " + obj.toString());
    		if(obj instanceof String) 
    			results.add((String)obj);
    		else if(obj instanceof History)
    			results.add(((History)obj).getMergedData("\t"));
    		else
    			results.add(obj.toString());
    	}
    	return results;
	}
	
	@Deprecated
	public static List<String> pipeOutputToStrings(Pipe<History, History> pipe) {
		ArrayList<String> lines = new ArrayList<String>();
		while(pipe.hasNext()) {
			History history = pipe.next();
			lines.add(history.getMergedData("\t"));
		}
		return lines;
	}

	@Deprecated
	public static List<String> pipeOutputToStrings2(Pipe<Object, String> pipe) {
		ArrayList<String> lines = new ArrayList<String>();
		while(pipe.hasNext()) {
			String str = pipe.next();
			lines.add(str);
		}
		return lines;
	}

	/**
	 * Print the output lines 
	 * @param lines Lines to print to stdout
	 */
	public static void printLines(List<String> lines) {
		for(String s : lines) {
			System.out.println(s);
		}
	}
	
    /**
     * Assert that two lists are equal (runs assertEquals on each line).  Prints any mismatched lines
     * @param expected The expected results
     * @param actual The actual results
     */
    public static void assertListsEqual(List<String> expected, List<String> actual) {
    	Assert.assertEquals("Array sizes are not equal!", expected.size(), actual.size());
    	for(int i=0; i < expected.size(); i++) {
    		Assert.assertEquals("Array item not equal!  Line: " + (i+1)
    				+ "\nExpected: " + expected.get(i)
    				+ "\nActual:   " + actual.get(i) + "\n",
    				expected.get(i),
    				actual.get(i));
    	}
    }
    
    /** Walk a pipeline and print out any subpipes, listing them in the hierarchy they are called */
    public static void walkPipeline(Pipeline pipeline, int depth) {
    	List<Pipe> pipes = pipeline.getPipes();
    	for(Pipe pipe : pipes) {
     		if( pipe instanceof Pipeline )
    			walkPipeline((Pipeline) pipe, depth+1);
    		else {
    			System.out.println(getDepthSpaces(depth) + pipe.getClass().getCanonicalName());

//    			for(Field field : pipe.getClass().getDeclaredFields()) {
//    			    System.out.println(field.getGenericType());
//    			}
    			
    			//    			Field[] fields = pipe.getClass().getDeclaredFields();//DPipeTestUtils.class.getDeclaredField("stringList");
//    			Annotation[] annotations = pipe.getClass().getAnnotations();
//    			Annotation[] declAnno = pipe.getClass().getDeclaredAnnotations();
//    			Field field = fields[0];
//    	        ParameterizedType type = (ParameterizedType) field.getGenericType();
//    	        Class<?> cls = (Class<?>)type.getActualTypeArguments()[0];
//    	        System.out.println(cls); // class java.lang.String.
    		}
    	}
    }
    
    private static String getDepthSpaces(int depth) {
    	StringBuilder str = new StringBuilder();
    	while(str.length() < depth) {
    		str.append("\t");
    	}
    	return str.toString();
    }

}
