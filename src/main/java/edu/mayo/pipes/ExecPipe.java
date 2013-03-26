/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

//import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
//import com.tinkerpop.pipes.filter.ComparisonFilterPipe;
import com.tinkerpop.pipes.filter.ObjectFilterPipe;
import com.tinkerpop.pipes.sideeffect.AggregatePipe;
import com.tinkerpop.pipes.sideeffect.SideEffectPipe;
//import com.tinkerpop.pipes.transform.InPipe;
//import com.tinkerpop.pipes.transform.OutPipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.exec.UnixStreamCommand;

import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * The exec pipe takes a string representing a command line in the constructor.
 * once it has this command, it creates another process that it will send data
 * to/get data from.
 * Input streams into the exec pipe as a list of lines at a time, and results stream
 * out of the exec pipe as a list of lines at a time.
 * For example, if the command passed to this pipe was blast, sequences would 
 * stream to the exec pipe (in fasta format e.g. >header\natgcattt...aaa\naat...agc\n)
 * the exec pipe will then send that data over to BLAST which is a seperate 
 * process running on the same system where it will get processed against
 * a blast database and then alignments will come out the other end as strings.
 * users will need to set up both a pre-processing pipeline to convert raw data into
 * a suitable format for processing with the algorithm and a post processing
 * pipeline to convert the data into something suitable for downstream usage.
 */
public class ExecPipe extends AbstractPipe<String, String> {
    
    private static final String[] ARGS = new String[0];
    private static final Map<String, String> NO_CUSTOM_ENV = new HashMap<String, String>(); 
    private boolean useParentEnv = true;
    private UnixStreamCommand cmd;
    private String commentSymbol = null;
  
    
    /**
     * 
     * @param cmdarray a string of values representing the command e.g. [grep] [-v] [foo]
     * @throws IOException 
     */
    public ExecPipe(String[] cmdarray) throws IOException {
        super();
        cmd = new UnixStreamCommand(cmdarray, NO_CUSTOM_ENV, true, true);
        //cmd = new UnixStreamCommand(cmdarray, NO_CUSTOM_ENV, true,  UnixStreamCommand.StdoutBufferingMode.LINE_BUFFERED, 0);
        cmd.launch();
    }
    
    /**
     * 
     * @param cmdarray a string of values representing the command e.g. [grep] [-v] [foo]
     * @param boolean usePrntEnv
     * @throws IOException 
     */
    public ExecPipe(String[] cmdarray, boolean useParentEnv) throws IOException{
        super();
        cmd = new UnixStreamCommand(cmdarray, NO_CUSTOM_ENV, useParentEnv, true);
        cmd.launch();
    }
    

    

    public String processNextStart() {
        try {
            StringBuilder sb = new StringBuilder();
            String line = this.starts.next();
            cmd.send(line);
            String output = cmd.receive();
            if(commentSymbol != null){
                while(output.startsWith(commentSymbol)){
                    sb.append(output);
                    sb.append("\n");
                    output = cmd.receive();
                }
            }
            sb.append(output);
            //System.out.println("EXECPIPE: " + sb.toString());   
            //sb.replace(i, i1, "");
            return sb.toString();
        } catch (InterruptedException ex) {
            Logger.getLogger(ExecPipe.class.getName()).log(Level.SEVERE, null, ex);
        } catch (BrokenBarrierException ex) {
            Logger.getLogger(ExecPipe.class.getName()).log(Level.SEVERE, null, ex);
        } catch (TimeoutException ex) {
            Logger.getLogger(ExecPipe.class.getName()).log(Level.SEVERE, null, ex);
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(ExecPipe.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ExecPipe.class.getName()).log(Level.SEVERE, null, ex);
            //shutdown();
            throw new NoSuchElementException();
        }
        //cmd.terminate();//needed? can this cause an error?
        throw new NoSuchElementException();
    }
    
    /**
     * shutdown terminates the child process
     */
    public void shutdown() throws InterruptedException, UnsupportedEncodingException{
        if(cmd!=null){
            cmd.terminate();
        }
    }
    
    

}
