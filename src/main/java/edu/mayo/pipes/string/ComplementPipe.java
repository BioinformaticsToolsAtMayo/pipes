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
public class ComplementPipe extends AbstractPipe<String, String>{

    @Override
    protected String processNextStart() throws NoSuchElementException {
        String s = this.starts.next();
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<s.length(); i++){
            char c = s.charAt(i);
            if(c=='A'){
                sb.append('T');
            }else if(c=='a'){
                sb.append('t');
            }else if(c=='C'){
                sb.append('G');
            }else if(c=='c'){
                sb.append('g');
            }else if(c=='G'){
                sb.append('C');
            }else if(c=='g'){
                sb.append('c');
            }else if(c=='T'){
                sb.append('A');
            }else if(c=='t'){
                sb.append('a');
            }else if(c=='N'){
                sb.append('N');
            }else if(c=='n'){
                sb.append('n');
            }else if(c=='X'){
                sb.append('X');
            }else if(c=='x'){
                sb.append('x');
            }else if(c=='U'){
                sb.append('A');
            }else if(c=='u'){
                sb.append('a');
            }
            
            else{
                throw new RuntimeException("ComplementPipe exception, character " + c + " has no complement!");
            }
        }
        return sb.toString();
    }
    
}
