/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import com.tinkerpop.pipes.AbstractPipe;
import java.io.File;
import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * given a directory on starts.next() -> return the contents of that directory
 * w/o . and ..
 * @author dquest
 */
public class LSPipe extends AbstractPipe<String, String>{
    boolean showHiddenFiles = true;
    boolean fullPath = false;
    public LSPipe(boolean showHiddenFiles){
        this.showHiddenFiles = showHiddenFiles;
    }
    
    /**
     * Calling fullPath before you use the pipe will cause ls to output full
     * file paths instead of relative ones.
     */
    public void fullPath(){
        fullPath = true;
    }
    
    @Override
    public boolean hasNext(){
        if(!this.files.isEmpty()) return true;
        if(this.starts.hasNext()) return true;
        return false;
    }

    ArrayList<String> files = new ArrayList();
    @Override
    protected String processNextStart() throws NoSuchElementException {
        while(!files.isEmpty()){
            return files.remove(0);
        }
        while(true){
            String directoryPath = (String) this.starts.next();
            File dir = new File(directoryPath);

            String[] f = dir.list();
            for(String file : f){
                String outpath = file;
                if(this.fullPath){
                    outpath = directoryPath + File.separator + file;
                }
                if(!file.equalsIgnoreCase(".") || !file.equalsIgnoreCase("..")){
                    if(!this.showHiddenFiles){
                        if(!file.startsWith(".")){
                            files.add(outpath);
                        }
                    }else {
                        files.add(outpath);
                    }
                }
            }
            if(files.size() == 0 && !this.starts.hasNext()){ return null; }
            return files.remove(0);
        }
    }
    
}
