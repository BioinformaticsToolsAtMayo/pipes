/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics.tools;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.SplitPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.WritePipe;
import edu.mayo.pipes.util.ProcessHandler;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 *
 * @author m102417
 */
public class ToolPipe extends AbstractPipe<List<String>, List<String>>{
        /**
     * Flow of execution:
     * 1. gather up maxbuffer lines from the input
     * 2. write maxbuffer lines to a file
     * 3. run the tool on the file > output.txt
     * 4. cat output.txt to outqueue
     * 5. when next() called, give next element from outqueue or run step 1.
     */
    
    protected String command = "perl variant_effect_predictor.pl --offline --config config.txt -i example.vcf ";
    protected String tempDir = "/tmp/";
    protected String inputFileFlag = "-i";
    protected String outputFileFlag = "-o";
    protected String outfile = "";
    protected String inputfile = "";
    //protected Pipe writeOutput = null;
    protected int maxbuffer = 10;
    protected LinkedList<List<String>> outqueue = new LinkedList();
    
    public ToolPipe(String tempdir){
        tempDir = tempdir;
        inputfile = getUniqueFileName(".in");
        outfile = getUniqueFileName(".out");
    }
    
    public String getUniqueFileName(String suffix){
        UUID idOne = UUID.randomUUID();
        String filename = tempDir + idOne.toString() + suffix;
        System.out.println(filename);
        return filename; 
        
    }
    
    
    /** 1. gather up maxbuffer lines from the input
     *  2. write maxbuffer lines to a file
     */
    public void makeInputFile() throws IOException{
        FileWriter fstream = new FileWriter(this.inputfile, false); //append should be false
        BufferedWriter f = new BufferedWriter(fstream);
        for(int i=0; i<this.maxbuffer && this.starts.hasNext();i++){
            List<String> next = this.starts.next();
            StringBuilder sb = new StringBuilder();
            for(int j=0; j<next.size(); j++){
                if(j>0 && j<next.size()){
                    sb.append("\t");
                }
                sb.append(next.get(j));
            }
            sb.append("\n");
            //System.out.println(sb.toString());
            f.write(sb.toString());
        }
        f.close();
    }
    
    /**
     * call only after pipeline has completed execution, this will clean up the temp files.
     */
    public void close(){
       try{
    		File f1 = new File(this.outfile);
                f1.delete();
                File f2 = new File(this.inputfile);
                f2.delete();
    	}catch(Exception e){
 
    		e.printStackTrace();
 
    	}
    }
    
    /** 3. run the tool on the file > output.txt */
    //We need to be able to run a command, redirecting standard error.  
    //If standard error shows something funny, then we need to bail out and show it to the user
    public void exec(String cmd, StringBuffer stdout, StringBuffer stderr ) throws IOException, InterruptedException{        
        //Note this code is a simple system call in java based on
        // http://www.devdaily.com/java/edu/pj/pj010016
        //It could be made more functional (and perhaps more confusing) by following
        // http://www.devdaily.com/java/java-exec-processbuilder-process-1
//               System.out.println("Running Command: " + cmd);
//               Process p;
//              try {
//                  p = Runtime.getRuntime().exec(cmd);
//                  BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
//                  BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
//                  String s = null;
//                  while ((s = stdInput.readLine()) != null) { System.out.println(s); }//we need to redirect this to logging... :(
//              } catch (IOException e) {
//                  e.printStackTrace();  
//              }
        System.out.println("Running Command: " + cmd);
        ProcessHandler.runCommand(cmd, stdout, stderr);
        
    }
    
    public void exec(String cmd) throws IOException, InterruptedException{
        ProcessHandler.runCommand(cmd);
    }
    
    /** 4. cat output.txt to outqueue */
    Pipe loadoutput = new Pipeline( new CatPipe(), new SplitPipe("\t")) ;
    public LinkedList loadfile(String filename){
        loadoutput.reset();
        loadoutput.setStarts(Arrays.asList(filename));
        while(loadoutput.hasNext()){
            outqueue.add((List<String>) loadoutput.next());
        }
        return outqueue;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public String getInputFileFlag() {
        return inputFileFlag;
    }

    public void setInputFileFlag(String inputFileFlag) {
        this.inputFileFlag = inputFileFlag;
    }

    public String getInputfile() {
        return inputfile;
    }

    public void setInputfile(String inputfile) {
        this.inputfile = inputfile;
    }

    public Pipe getLoadoutput() {
        return loadoutput;
    }

    public void setLoadoutput(Pipe loadoutput) {
        this.loadoutput = loadoutput;
    }

    public int getMaxbuffer() {
        return maxbuffer;
    }

    public void setMaxbuffer(int maxbuffer) {
        this.maxbuffer = maxbuffer;
    }

    public String getOutfile() {
        return outfile;
    }

    public void setOutfile(String outfile) {
        this.outfile = outfile;
    }

    public String getOutputFileFlag() {
        return outputFileFlag;
    }

    public void setOutputFileFlag(String outputFileFlag) {
        this.outputFileFlag = outputFileFlag;
    }

    public String getTempDir() {
        return tempDir;
    }

    public void setTempDir(String tempDir) {
        this.tempDir = tempDir;
    }

    @Override
    protected List<String> processNextStart() throws NoSuchElementException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    
}
