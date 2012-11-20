/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.NoSuchElementException;


/**
 * Print pipe passes along the object and prints it to the screen
 * otherwise it is an identity pipe
 * @author dquest
 */
public class PrintPipe extends AbstractPipe<Object, Object> {

    public PrintPipe(){//TODO: allow us to pass streams in...

    }
    private void printStringArray(String[] s){
        String pm = "[";
        for(int i=0;i<s.length;i++){
            pm += s[i] + ",";
        }
        pm += "]";
        System.out.println(pm);
    }

    protected Object processNextStart() throws NoSuchElementException {
            Object o = this.starts.next();
            if(o instanceof String[]){
                printStringArray((String[])o);
            }else {
                System.out.println(o.toString());
            }
            return o;
    }
    
}
