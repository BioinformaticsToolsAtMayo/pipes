/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.history;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.NoSuchElementException;

/**
 * This pipe works on a history (list of strings)
 * This Pipe takes an index (e.g. 8 for column 8) in its constructor
 * It will then do a find and replace on that column.
 * (findRegex is the regular expression for a match)
 * (replaceRegex is the regular expression for what should be placed in the column instead)
 * If the resulting column is empty, it will return a period (.)
 * 
 * The semantics for the index are the same as the -c option in many other pipes:
 * e.g. given a list a of size 5,
 * array = index+ = index-
 * a[0] = 1 = -5
 * a[1] = 2 = -4
 * a[2] = 3 = -3
 * a[3] = 4 = -2
 * a[4] = 5 = -1
 * 
 * 
 * @author dquest
 */
public class FindAndReplaceHPipe extends AbstractPipe<History, History>{
    private int index = -1;
    private String findRegex = "";
    private String replaceRegex = "";
    
    public FindAndReplaceHPipe(int index, String findRegex, String replaceRegex){
        this.findRegex = findRegex;
        this.replaceRegex = replaceRegex;
        this.index = index;
    }
    
    private int fixIndex(History h){
        if(index >0){
            index = index - h.size() -1;
            return index;
        }else {
            return index;
        }       
    }
    
    private History doReplace(History h){
        int pos = h.size()+index;
        String s = h.get(pos);
        String replace = s.replaceAll(findRegex, replaceRegex);
        h.set(pos, replace);
        return h;
    }

    @Override
    protected History processNextStart() throws NoSuchElementException {
        History h = this.starts.next();
        fixIndex(h);
        doReplace(h);
        return h;
    }
    
}
