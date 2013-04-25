/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes;

//import com.tinkerpop.blueprints.pgm.Vertex;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.exec.AbnormalExitException;
import edu.mayo.exec.UnixStreamCommand;


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
    
	private Logger sLogger = Logger.getLogger(ExecPipe.class);
	
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
    
    
        /**
     * 
     * @param cmdarray a string of values representing the command e.g. [grep] [-v] [foo]
     * @param boolean usePrntEnv
     * @param commentSymbol - a string that we should ignore the contents of if this symbol is at the start
     * @throws IOException 
         * @throws AbnormalExitException 
     */
    public ExecPipe(String[] cmdarray, boolean useParentEnv, String commentSymbol) throws IOException, InterruptedException, BrokenBarrierException, TimeoutException, AbnormalExitException{
        super();
        this.commentSymbol = commentSymbol;
        cmd = new UnixStreamCommand(cmdarray, NO_CUSTOM_ENV, useParentEnv, true);
        cmd.launch();
        cmd.send("#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n" +
                    "chr1	949608	rs0001	G	A	.	.	.");
        cmd.receive();
        cmd.receive();
        cmd.receive();
        cmd.receive();
        cmd.receive();
        
    }

    

    public String processNextStart() {
        try {
            String line = this.starts.next();
            //sLogger.info("EXECPIPE: " + line);
            cmd.send(line);
            String output = cmd.receive();
            //sLogger.info("EXEKEEP: " + output);
            return output;
        } catch (InterruptedException ex) {
        	sLogger.error(ex.getMessage(), ex);            
        } catch (BrokenBarrierException ex) {
        	sLogger.error(ex.getMessage(), ex);
        } catch (TimeoutException ex) {
        	sLogger.error(ex.getMessage(), ex);
        } catch (UnsupportedEncodingException ex) {
        	sLogger.error(ex.getMessage(), ex);
        } catch (IOException ex) {
        	sLogger.error(ex.getMessage(), ex);
            //shutdown();
            throw new NoSuchElementException();
        } catch (AbnormalExitException ex) {
        	sLogger.error(ex.getMessage(), ex);
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
