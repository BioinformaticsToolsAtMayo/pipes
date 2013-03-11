package edu.mayo.pipes.iterators;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;

/**
 *
 * @author dquest
 */
public class FileLineIterator implements Iterator<String> {  
//    private FileInputStream fis;
    protected BufferedReader br;
//    private InputStreamReader isr;
//    private String filename = null;	// Never used
    private LinkedList<String> queue = new LinkedList<String>();
    private boolean readok = true;
    
    public FileLineIterator(String filename) throws IOException {        
        open(filename);
    }

    public boolean hasNext() {
    	if(queue.size() > 0){ return true; }
    	if(readok == false){ return false; }
    	String line;
		try {
			if ((line = br.readLine()) != null) {
				queue.add(line);
				return true;
			} else {
				readok = false;
            }
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
    }

    public String next() {
    	if(queue.size() < 1){
			this.hasNext();
        }
        return queue.pollFirst();
    }

    public void remove() {
        return;
    }
    
    public BufferedReader open(String filename) throws IOException{
    	readok = true;
//        this.filename = filename;
//        fis = new FileInputStream(filename);	
//        isr = new InputStreamReader(fis);
//        br = new BufferedReader(isr);
        br = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
        return br;
    }
    
    public void close() throws IOException{
    	br.close();
//    	isr.close();	// Closing a BufferedReader closes everything it came from, too
//    	fis.close();
    	readok = false;
    }    
}
