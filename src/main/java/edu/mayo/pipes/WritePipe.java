package edu.mayo.pipes;

import com.tinkerpop.pipes.AbstractPipe;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  Write Pipe takes a constructor of a filename for the file we wish to write
 * and when strings stream to the write pipe, it appends a newline ('\n')
 * as it writes each string to the file. Each string written to the file
 * is then allowed to pass down the pipeline.  
 * @author dquest
 */
public class WritePipe extends AbstractPipe<String, String>{
    
    private BufferedWriter out = null; 
    private boolean mIsAppendToFile = true;
    private boolean mIsAddNewLine = false;
    public WritePipe(String filename){
    	this(filename, false, false);
    }

    public WritePipe(String filename, boolean isAppendToFile) {
    	this(filename, isAppendToFile, false);
    }
    
    public WritePipe(String filename, boolean isAppendToFile, boolean isAddNewlines){
        this.mIsAppendToFile = isAppendToFile;
        this.mIsAddNewLine = isAddNewlines;
        try {
                out = new BufferedWriter(new FileWriter(filename, mIsAppendToFile));
        } catch (IOException e) {
        }
    }
    
    public void close() throws IOException{
        out.close();
    }

    @Override
    protected String processNextStart() throws NoSuchElementException {
        try {
            if( ! this.starts.hasNext()) {
            	out.close();
            	throw new NoSuchElementException();
            }
            String s = this.starts.next();
            out.write(s + (mIsAddNewLine ? "\n" : ""));
            return s;
        } catch (IOException ex) {
            Logger.getLogger(WritePipe.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
}
