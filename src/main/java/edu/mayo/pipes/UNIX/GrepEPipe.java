/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.filter.FilterPipe;

/**
 * This pipe is the negative of GrepPipe (that works just like the unix grep utility), 
 * in that it will only return results that do NOT match the given regular expression
 * See:
 *   http://stackoverflow.com/questions/10198713/how-to-find-out-if-a-substring-of-a-java-string-matches-a-regular-expression
 *   http://javamex.com/tutorials/regular_expressions/repetition_greedy_reluctant.shtml#.UQgvNegzKts
 * @author dquest
 */
public class GrepEPipe extends AbstractPipe<String, String> implements FilterPipe<String> {
    private String regex = "";
    public GrepEPipe(String regex){
        this.regex = regex;
    }
    protected String processNextStart() {
        while (this.starts.hasNext()) {
            String s = this.starts.next();
            // Use a reluctant operator on each side of the supplied regex 
            // expression so it doesn't have to match the entire string
           if (!s.matches(".*?" + regex + ".*?")) {
                return s;
            }
        }
        throw new NoSuchElementException();
    }
    
}
