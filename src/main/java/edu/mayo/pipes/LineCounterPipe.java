package edu.mayo.pipes;

import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.history.History;
import java.util.NoSuchElementException;

/**
 * Count the number of lines coming into this pipe
 * @author Mike Meiners
 */
public class LineCounterPipe<S> extends AbstractPipe<History, History> {
	private long mNumLines = 0;
	
    protected History processNextStart() throws NoSuchElementException {
        History h = this.starts.next();
        mNumLines++;
        return h;
    }

    public long getLineCount() {
        return mNumLines;
    }
}