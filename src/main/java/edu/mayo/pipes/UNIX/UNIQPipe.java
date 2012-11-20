/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 *  Warning, you have to put everything into ram to make this work.
 *  Otherwise it works like the unix utility uniq.
 * @author m102417
 */
public class UNIQPipe extends AbstractPipe<String, String>{
    private Set<String> uniqueElements = new HashSet<String>();
    private Iterator<String> iterator = null;

    @Override
    protected String processNextStart() throws NoSuchElementException {
        while(this.starts.hasNext() && iterator == null){
            String s = this.starts.next();
            uniqueElements.add(s);
        }
        if(iterator == null){
            iterator = uniqueElements.iterator();
        }
        if(iterator.hasNext()){ return iterator.next(); }
        else throw new NoSuchElementException();
    }
    
}
