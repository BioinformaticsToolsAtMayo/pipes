/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.sideEffect;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import com.rits.cloning.*;

/**
 *
 * @author m102417
 */
public class CachePipe<S> extends AbstractPipe<S,S> {
    
    private S cache = null;
    
    public S get(){
        if(cache == null){
            throw new NoSuchElementException();
        }
        return cache;
    }

    @Override
    protected S processNextStart() throws NoSuchElementException {
        cache = new Cloner().deepClone(this.starts.next());
        return cache;
    }
    
}
