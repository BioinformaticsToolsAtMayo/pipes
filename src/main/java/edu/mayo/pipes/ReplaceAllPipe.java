/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.NoSuchElementException;

/**
 * 
 * @author m102417
 */
public class ReplaceAllPipe extends AbstractPipe<String, String>{
    private String input;
    private String output;
    public ReplaceAllPipe(String input, String output){
        this.input = input;
        this.output = output;
    }

    @Override
    protected String processNextStart() throws NoSuchElementException {
        String s = this.starts.next();
        return s.replaceAll(input, output);
    }
    
}
