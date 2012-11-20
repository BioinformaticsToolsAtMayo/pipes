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
 * This pipe is the negative of GrepPipe that works just like the unix grep utility
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
            if (!s.matches(regex)) {
                return s;
            }
        }
        throw new NoSuchElementException();
    }
    
}