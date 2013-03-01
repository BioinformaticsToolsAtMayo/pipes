/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.AbstractMetaPipe;
import com.tinkerpop.pipes.util.MetaPipe;
import com.tinkerpop.pipes.util.Pipeline;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *
 * @author m102417
 */
public class CatAnythingPipe<S,E> extends AbstractMetaPipe<S, E> implements MetaPipe {

    private final Pipe<S, E> pipe;

//    public AsPipe(final String name, final Pipe<S, E> pipe) {
//        this.pipe = pipe;
//        this.name = name;
//    }
    public CatAnythingPipe(String type){
        if(type.equalsIgnoreCase("gzip")){
            this.pipe = (Pipe<S, E>) new CatGZPipe("gzip");
        }else{
            this.pipe = (Pipe<S, E>) new CatPipe();
        }
    }

    public void setStarts(final Iterator<S> starts) {
        this.pipe.setStarts(starts);
        this.starts = starts;
    }

    public E getCurrentEnd() {
        return this.currentEnd;
    }

    public E processNextStart() {
        return this.pipe.next();
    }

    public List<Pipe> getPipes() {
        return (List) Arrays.asList(this.pipe);
    }

}
