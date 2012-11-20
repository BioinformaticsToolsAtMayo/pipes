/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.*;

/**
 * A Drain Pipe takes a collection and 'drains' it emiting each object
 * until the collection is empty
 * @author dquest
 */
public class DrainPipe<S, E> extends AbstractPipe<S,E> {

    ArrayList<E> outqueue = new ArrayList<E>();
    
    @Override
    protected E processNextStart() throws NoSuchElementException {
        if(outqueue.size() > 0){
            return (E) outqueue.remove(0);          
        }
        if(this.starts.hasNext()){
            S o = (S) this.starts.next();
            if(o instanceof Collection){
                Iterator<E> i = ((Collection) o).iterator();
                while(i.hasNext()){
                    outqueue.add(i.next());
                }
            }
            if(o instanceof Object[]) {
                E[] arr = (E[])o;
                outqueue.addAll(Arrays.asList(arr));
            }
            return (E) outqueue.remove(0); 
        }else {
            throw new NoSuchElementException();
        }
    }
    
    
}
