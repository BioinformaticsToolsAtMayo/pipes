package edu.mayo.pipes.iterators;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.zip.GZIPInputStream;

public class GZipFileLineIterator implements Iterator {
	static final int BUFFER = 2048;
	GZIPInputStream gzip;
	FileInputStream fis;
	BufferedReader br;
	InputStreamReader isr;
	String filename = null;
	LinkedList<String> queue = new LinkedList();
	private boolean readok = true;
	
	public GZipFileLineIterator(String filename) throws IOException{
		this.open(filename);
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
		this.filename = filename;
		fis = new FileInputStream(filename);
		gzip = new GZIPInputStream(fis);	
		isr = new InputStreamReader(gzip);
		br = new BufferedReader(isr);
		return br;
	}
	
	public void close() throws IOException{
		br.close();
		isr.close();
		gzip.close();
		fis.close();
        readok = false;
	}
}
