/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.filter.FilterPipe;
import java.util.NoSuchElementException;

/**
 * The header pipe passes along all lines but the first n lines - which it emits as a 
 * side effect
 * @author Daniel Quest
 */
public class HeaderPipe<S> extends AbstractPipe<String, String> implements FilterPipe<String> {
    
    /**
     * n - number of lines to skip
     * @param n 
     */
    private String header = "";
    private int n = 0;
    private int count = 0;
    public HeaderPipe(int n) {
        this.n = n;
    }
    
    protected String processNextStart() throws NoSuchElementException {
            String s = (String) this.starts.next();
            while(n > count){
                count++;
                header += s + "\n";
                s = (String) this.starts.next();
                if(n == count+1){
                    //header += s+ "\n";
                }
            }
            return s;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }
    
    
}