/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import edu.mayo.pipes.UNIX.CatPipe;
import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.filter.FilterPipe;
import edu.mayo.pipes.iterators.FileLineIterator;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Works like the UNIX utility head... filenames come in, and the top n lines from
 * the file come out.
 * @author dquest
 */
public class HeadPipe extends CatPipe implements FilterPipe<String> {
    
    private int filesProcessed = 0;
    private int n = 1;
    private int linesProcessed = 0;
    private CatPipe cat = new CatPipe();
    /**
    * n is the number of lines you want to pipe out for each file
    * @param n 
    */
    public HeadPipe(int n){
        this.n = n;
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
        if(cat.getFilesProcessed() > this.filesProcessed){
            this.filesProcessed++;
            linesProcessed = 0;
        }
        linesProcessed++;
        if(linesProcessed <= n)
            return cat.next();
        else 
            return null;
    }

    
    
}
