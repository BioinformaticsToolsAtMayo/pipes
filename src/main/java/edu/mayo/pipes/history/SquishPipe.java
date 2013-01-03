/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.history;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.NoSuchElementException;

/**
 * Sometimes a set of pipes creates replicated rows e.g.
22	45312244	rs6519902	G	A	.	.	.	.	.	.	.
22	45312244	rs6519902	G	A	.	.	.	.	.	.	.
22	45312244	rs6519902	G	A	.	.	.	.	.	.	.
22	45312244	rs6519902	G	A	.	.	.	.	.	.	.
22	45312244	rs6519902	G	A	.	.	.	.	.	.	.
22	45312244	rs6519902	G	A	.	.	.	.	.	.	.
...
* The SquishPipe works as follows, as rows come in it takes the first n columns and compares them to the
* previous history (list of strings) emited.  If it matches, it filters our that row (drops it)
* if it does not match, it passes it to it's sink.
 * 
 * @author dquest
 */
public class SquishPipe extends AbstractPipe<History, History> {
    private History prev = null;
    private int n = 1;
    SquishPipe(int numberPrevColSame){
        n = numberPrevColSame;
    }
    
    private boolean sameN(History hist){
        for(int i=0; i<=n; i++){
            String h = hist.get(i);
            String p = prev.get(i);
            if(!h.equals(p)){
                return false;
            }
        }
        return true;
    }

    @Override
    protected History processNextStart() throws NoSuchElementException {
        History h = this.starts.next();
        if(prev == null){
            prev = (History) h.clone();
            return h;
        }
        if(sameN(h)){
            return processNextStart();
        }else {
            prev = (History) h.clone();
            return h;
        }
    }
    
}
