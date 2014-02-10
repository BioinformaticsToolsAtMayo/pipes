package edu.mayo.pipes;

import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

/**
 * Count the number of lines coming into this pipe
 * @author Mike Meiners
 */
public class LineCounterPipe<S> extends AbstractPipe<S, S> {
	private long mNumLines = 0;
	
    protected S processNextStart() throws NoSuchElementException {
        S line = this.starts.next();
        mNumLines++;
        return line;
    }

    public long getLineCount() {
        return mNumLines;
    }
}