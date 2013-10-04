/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.string;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.NoSuchElementException;

/**
 *
 * @author m102417
 */
public class ReversePipe extends AbstractPipe<String, String>{

    public String reverse(String s){
        StringBuilder sb = new StringBuilder();
        for(int i=s.length()-1; i>=0; i--){
            sb.append(s.charAt(i));
        }
        return sb.toString();
    }

    @Override
    protected String processNextStart() throws NoSuchElementException {
        String s = this.starts.next();
        return reverse(s);
    }
    
}
