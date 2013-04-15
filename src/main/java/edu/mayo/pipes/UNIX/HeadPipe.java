/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.Pipe;

/**
 * Works like the UNIX utility head... filenames come in, and the top n lines from
 * the file come out.
 * @author dquest
 */
public class HeadPipe extends AbstractPipe<String,String> {
    
    private int filesProcessed = 0;
    private int n = 1;
    private int linesProcessed = 0;
    private Pipe cat = new CatPipe();
    /**
    * n is the number of lines you want to pipe out for each file
    * @param n 
    */
    public HeadPipe(int n){
        this.n = n;
    }
    
    public HeadPipe(int n, Pipe input) throws ClassCastException{
        this.n = n;
        cat = input;
//        if(input instanceof CatPipe){
//            cat = (CatPipe) input;
//        }if(input instanceof CatGZPipe){
//            cat = (CatGZPipe) input;
//        }else{
//            throw new ClassCastException("Head Pipe must have a CatPipe or a CatGZPipe passed to it");
//        }
    }
    
    @Override
    public boolean hasNext(){
        cat.setStarts(this.starts);
        if(linesProcessed < n)
            return cat.hasNext();
        else
            return false;
    }

    @Override
    protected String processNextStart() throws NoSuchElementException {
        cat.setStarts(this.starts);
//        if(cat.getFilesProcessed() > this.filesProcessed){
//            this.filesProcessed++;
//            linesProcessed = 0;
//        }
        linesProcessed++;
        if(linesProcessed <= n)
            return (String) cat.next();
        else 
            throw new NoSuchElementException();
    }

    
    
}
