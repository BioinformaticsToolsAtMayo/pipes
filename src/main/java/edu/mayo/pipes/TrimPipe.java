/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.NoSuchElementException;

/**
 * A TrimPipe trims off leading and trailing spaces from data.
 * @author dquest
 */
public class TrimPipe extends AbstractPipe<String, String> {

    @Override
    protected String processNextStart() throws NoSuchElementException {
        return this.starts.next().trim();
    }

}