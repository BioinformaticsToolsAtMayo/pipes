/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.filter.FilterPipe;
import java.util.HashSet;
import java.util.NoSuchElementException;



/**
 *
 * @author m102417
 * GrepListOnSetPipe takes a set in the constructor and a position (column)
 * Takes a string array 
 */
public class GrepListOnSetPipe extends AbstractPipe<String[], String[]> implements FilterPipe<String[]> {
    private HashSet<String> set = null;
    private int position = 0;
    public GrepListOnSetPipe(HashSet<String> s, int pos){
        position = pos;
        set = s;
    }

    @Override
    protected String[] processNextStart() throws NoSuchElementException {
        while (this.starts.hasNext()) {
            String s[] = this.starts.next();
            if(set.contains(s[position])){
                return s;
            }
        }
        throw new NoSuchElementException();
    }
    
}
