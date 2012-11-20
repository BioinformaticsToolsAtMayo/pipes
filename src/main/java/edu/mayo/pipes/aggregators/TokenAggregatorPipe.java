/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.aggregators;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;

/**
 * A token aggregator Pipe is initialized with a token (e.g. ">" in a fasta file)
 * it will aggrigate lines into a collection until it encounters the next instance
 * of the token, then it will send the collection of lines along, and start the next
 * aggregation
 * @author m102417
 */
public class TokenAggregatorPipe extends AbstractPipe<String, ArrayList<String>> {
    public TokenAggregatorPipe(String token){
        this.token = token;
    }
    private String token;
    private ArrayList<String> objs = null;
    private String buffer = "";

   @Override
    protected ArrayList<String> processNextStart() throws NoSuchElementException {
        this.objs = new ArrayList<String>();
        if(buffer.length()>0){
            this.objs.add(buffer);
            buffer = "";
        }
	for(int i=0; this.starts.hasNext(); i++) {
            String s = this.starts.next();
            if(objs.isEmpty()){
		objs.add(s);  
            }else if(s.contains(token)){
                buffer = s;
                return this.objs;
            }else {
                objs.add(s); 
            }
                
	}
        //If there is no objects in the return list, then throw exe
        if(this.objs.size() < 1){ throw new NoSuchElementException(); }
	return this.objs;
	
    }


}
