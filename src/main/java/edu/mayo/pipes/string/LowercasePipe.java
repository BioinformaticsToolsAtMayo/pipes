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
public class LowercasePipe extends AbstractPipe<String, String>{

    @Override
    protected String processNextStart() throws NoSuchElementException {
        return this.starts.next().toLowerCase();
    }
    
}
