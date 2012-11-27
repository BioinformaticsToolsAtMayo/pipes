/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *  MergePipe takes a list of strings, and merges them into a single string.
 * @author m102417
 */
public class MergePipe extends AbstractPipe<List<String>, String>{
    private String delimiter = "\t";
    public MergePipe(String delim){
        this.delimiter = delim;
    }

    @Override
    protected String processNextStart() throws NoSuchElementException {
        if(this.starts.hasNext()){
            StringBuilder sb = new StringBuilder();
            List<String> l = this.starts.next();
            if(l.size()==1){
                return l.get(0);
            }
            for(int i=0;i<l.size()-1;i++){
                String s = l.get(i);
                sb.append(s);
                sb.append(delimiter);
            }
            sb.append(l.get(l.size()-1));
            return sb.toString();
        }else {
            throw new NoSuchElementException();
        }
        
    }
    
}
