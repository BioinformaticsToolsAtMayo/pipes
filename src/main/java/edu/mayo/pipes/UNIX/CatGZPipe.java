package edu.mayo.pipes.UNIX;

import java.io.IOException;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

import edu.mayo.pipes.iterators.GZipFileLineIterator;

/**
 * ZippedFile2LinePipe takes as input a string describing a filename
 * and outputs, line by line, the contents of the file.  It leaves the file in place zipped
 * @author dquest
 *
 */
public class CatGZPipe extends AbstractPipe<String, String>{
	private String compressionType = "gzip"; //only supporting gzip for now...
    protected String nextLine = null;
	
    /** Takes the type of compression used to zip the target file (only "gzip" supported for now) */
    public CatGZPipe(String compression){
		this.compressionType = compression;
	}
        
    GZipFileLineIterator gzfile = null;
    
    private GZipFileLineIterator openFile(String filename) throws IOException{
        gzfile = new GZipFileLineIterator(filename);
        return gzfile;
    }
    
    @Override
    protected String processNextStart() throws NoSuchElementException {
    	if(gzfile == null){
    		try {
    			openFile(this.starts.next());
            } catch (Exception e) {
            	throw new NoSuchElementException(e.getMessage());
            }
        }
        
    	if(gzfile.hasNext()) {
    		return gzfile.next();
        } else {
        	//get from the next file......
            try {
            	gzfile = openFile(this.starts.next());
                
            	if(gzfile.hasNext()){
            		return gzfile.next();
                }
            } catch (Exception e) {
            	throw new NoSuchElementException(e.getMessage());
            }
        }
        
    	throw new NoSuchElementException();
    } 
          
        
//	public String processNextStart2(){
//		try {
//			String line;
//			if(filename == null){
//					System.out.println("new pipeline...");
//					filename = this.starts.next();
//					open(filename);
//					this.queue.add(br.readLine());
//					this.processNextStart();
//			}
//			if ((line = br.readLine()) != null) {
//				System.out.println("keep reading from file...");
//				this.queue.add(line);
//				this.processNextStart();
//			}else {//
//				    System.out.println("open new file, close current file...");
//					close();
//					filename = this.starts.next();
//					open(filename);
//					this.queue.add(br.readLine());
//					this.processNextStart();
//			}
//		} catch (IOException e) {
//			//TODO: create log statement containing: "ZippedFile2LinePipe.processNextStart open(" + filename + ") failed does file exist?"
//			e.printStackTrace();
//		}
//		return null;
//	}

//	protected String processNextStart() {
//		if (this.queue.isEmpty()) {
//			filename = this.starts.next();
//			this.queue.add(filename);
//            this.processNextStart();
//        } else {
//        	return (String) this.queue.remove();
//        }
//		return null;
//	}
	
/*
        boolean done = false;
        @Override
        public boolean hasNext(){
            if(done == true){
                return false;
            }
            if(this.starts.hasNext()){ // || this.gzfile.hasNext()
                return true;
            }
            if(this.gzfile.hasNext()){
                return true;
            }
            return false;
        }
	
	protected GZipFileLineIterator gzfile = null;
	private String filename = null;
    protected String processNextStart() {	
    	if(this.gzfile == null){
			try {
				filename = this.starts.next();
				gzfile = new GZipFileLineIterator(filename);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
        while (this.hasNext()) { //this.gzfile.hasNext() || this.starts.hasNext()
            System.out.println("The While loop grinds??");
            if (this.gzfile.hasNext()) {
                return this.gzfile.next();
            } else {
            	try {
            		if(gzfile != null){ gzfile.close(); }
            		filename = this.starts.next();
                        if(filename == null){ throw new NoSuchElementException(); }
                        System.out.println(filename);
			gzfile = new GZipFileLineIterator(filename);
                        if(gzfile.hasNext()){
                            return gzfile.next();
                        }
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                //this.nextEnds = this.starts.next().getVertices(this.direction, this.labels).iterator();
            }
        }
        
        throw new NoSuchElementException();
           
    }

    public void close() throws IOException {
        this.gzfile.close();
    }
*/


}
