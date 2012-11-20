/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.NoSuchElementException;

/**
 * A Pick Pipe selects a certain column from a String[] e.g. String[2]
 * @author dquest
 */
public class PickPipe extends AbstractPipe<String[], String> {

    int n;
            
    public PickPipe(int column){
        n = column;
    }
    @Override
    protected String processNextStart() throws NoSuchElementException {
        String[] s = this.starts.next();
        if(s.length < n){
            throw new NoSuchElementException();
        }
        return s[n];
    }

}
    
