/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.NoSuchElementException;

/**
 * Takes a string in, adds the token, and passes out the concatinated version of the string
 * @author m102417
 */
public class AppendStringPipe extends AbstractPipe<String, String>{

    String append = "";
    public AppendStringPipe(String appendMe){
        this.append = appendMe;
    }
    
    @Override
    protected String processNextStart() throws NoSuchElementException {
        return this.next().toString() + append;
    }
    
}
