/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics.tools;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.ExecPipe;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.WritePipe;
import edu.mayo.pipes.util.ProcessHandler;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.*;

/**
 *
 * @author Daniel Quest
 * VEP stands for Variant Effect Predictor.
 * For this pipe to work VEP needs to be installed and in your path.
 */
public class VEPipe extends ToolPipe{  //don't have access to the history code, just doing this for now...   
    
    public VEPipe(String tmpDir){
        super(tmpDir);
    }

   
    @Override
    protected List<String> processNextStart() throws NoSuchElementException {
        //fill up a buffer full of records to process by the tool
        //write.setStarts(this.starts);
        //ArrayList al = new ArrayList();
        //for(int i=0; this.starts.hasNext() && i<maxbuffer; i++){
        //    al.add(this.starts.next());
        //}
        //write the records to a temp file for processing.
        //mergeWrite.reset();
        //mergeWrite.setStarts(al);
        //while(mergeWrite.hasNext()){
        //    mergeWrite.next();
        //}
        //execute the command
        //exe.reset();
        //exe.setStarts(Arrays.asList(this.command));
        //exe.next();
        
        //if the queue has data to send along, then send it...
        if(outqueue.size() > 0){
            return outqueue.pollFirst();
        }else {
        //the queue does not have data, we need to try to populate it.
            try {
                makeInputFile();
                exec(this.command); 
            //loadfile(this.outfile);
            //processNextStart();
            }catch (Exception e){
                //these exceptions are the bad case!
                e.printStackTrace();
            }
        }
        
        throw new NoSuchElementException();
    }
    
}
