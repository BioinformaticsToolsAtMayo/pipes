/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.filter.FilterFunctionPipe;
import com.tinkerpop.pipes.filter.FilterPipe;
import java.util.NoSuchElementException;

/**
 * This pipe works just like the unix grep utility
 * See:
 *   http://stackoverflow.com/questions/10198713/how-to-find-out-if-a-substring-of-a-java-string-matches-a-regular-expression
 *   http://javamex.com/tutorials/regular_expressions/repetition_greedy_reluctant.shtml#.UQgvNegzKts
 * @author dquest
 */
public class GrepPipe extends AbstractPipe<String, String> implements FilterPipe<String> {
    private String regex = "";
    public GrepPipe(String regex){
        this.regex = regex;
    }
    protected String processNextStart() {
        while (this.starts.hasNext()) {
            String s = this.starts.next();
            // Use a reluctant operator on each side of the supplied regex 
            // expression so it doesn't have to match the entire string
            if(s.matches(".*?" + regex + ".*?")) {
                return s;
            }
        }
        throw new NoSuchElementException();
    }
    
}
