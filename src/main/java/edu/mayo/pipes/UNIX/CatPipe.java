package edu.mayo.pipes.UNIX;

import java.io.IOException;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.iterators.FileLineIterator;
import edu.mayo.pipes.iterators.GZipFileLineIterator;

/**
 * ZippedFile2LinePipe takes as input a string describing a filename
 * and outputs, line by line, the contents of the file.  It leaves the file in place zipped
 * @author dquest
 *
 */
public class CatPipe extends AbstractPipe<String, String> {
	
	public CatPipe(){
    }	
	
	private int filesProcessed = 0;
	
	private FileLineIterator fileitter = null;
	
	private FileLineIterator openFile(String filename) throws IOException{
		fileitter = new FileLineIterator(filename);
        return fileitter;
    }
	
	
    protected String processNextStart1() throws NoSuchElementException {	
        if(this.fileitter == null){
            try {
            	fileitter = openFile(this.starts.next());
                filesProcessed++;
            } catch (Exception e) {
            	throw new NoSuchElementException(e.getMessage());
            }
        
        } //else {

        //while (true) { 
        	if (this.fileitter.hasNext()) {
	        	return (String) this.fileitter.next();
	        } else {
	            try {	            	
	            	if (this.starts!=null && this.starts.hasNext()) {
	            		fileitter = openFile(this.starts.next());	            		
	            		filesProcessed++;	            		
	            	} else {
	            		throw new NoSuchElementException();
	            	}
		            	
	            	if(fileitter.hasNext()){
	            		return (String) fileitter.next();
	                }
	            } catch (Exception e) {
	            	throw new NoSuchElementException(e.getMessage());
	            }
	        }	        
        //}
        	throw new NoSuchElementException();
        //}	         
    }
	

	@Override
    protected String processNextStart() throws NoSuchElementException {	
        if(fileitter == null){
            try {
            	fileitter = openFile(this.starts.next());
                filesProcessed++;
            } catch (Exception e) {
            	throw new NoSuchElementException(e.getMessage());
            }        
        }  
        	
        	if (fileitter.hasNext()) {
	        	return (String) fileitter.next();
	        } else {
	            try {	
	            	if(fileitter != null){ fileitter.close(); }
	            	if (this.starts!=null) {	            		
	            		fileitter = openFile(this.starts.next());	            		
	            		filesProcessed++;	            		
	            	} else {
	            		throw new NoSuchElementException();
	            	}
		            	
	            	if(fileitter.hasNext()){
	            		return (String) fileitter.next();
	                }
	            } catch (Exception e) {
	            	throw new NoSuchElementException(e.getMessage());
	            }
	        }	        
        
        	throw new NoSuchElementException();
        	         
    }

	
    
    
    /*
    protected String processNextStart() throws NoSuchElementException {	
        if(this.fileitter == null){
            try {
                filename = this.starts.next();
                fileitter = new FileLineIterator(filename);
                filesProcessed++;
            } catch (Exception e) {
            	throw new NoSuchElementException(e.getMessage());
            }
        }
        
        while (true) { 
            if (this.fileitter.hasNext()) {
                return (String) this.fileitter.next();
            } else {
                try {
                        if(fileitter != null){ fileitter.close(); }
                        filename = this.starts.next();
                        fileitter = new FileLineIterator(filename);
                        filesProcessed++;
                } catch (Exception e) {
                	throw new NoSuchElementException(e.getMessage());
                }
                //this.nextEnds = this.starts.next().getVertices(this.direction, this.labels).iterator();
            }
        }
    }
	*/
	
    public int getFilesProcessed() {
        return filesProcessed;
    }

        
}
