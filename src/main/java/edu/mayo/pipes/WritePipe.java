/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
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
    
    BufferedWriter out = null; 
    public WritePipe(String filename){
        try {
                out = new BufferedWriter(new FileWriter(filename));
                //out.write("aString");
        } catch (IOException e) {
        }
    }
    
    public void close() throws IOException{
        out.close();
    }

    @Override
    protected String processNextStart() throws NoSuchElementException {
        try {
            if(!this.starts.hasNext()){out.close();}
            String s = this.starts.next();
            out.write(s);
            return s;
        } catch (IOException ex) {
            Logger.getLogger(WritePipe.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }


    
}
