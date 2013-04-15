/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.NoSuchElementException;

import org.biojava.bio.BioException;
import org.biojavax.SimpleNamespace;
import org.biojavax.bio.seq.RichSequence;
import org.biojavax.bio.seq.RichSequenceIterator;

import com.tinkerpop.pipes.AbstractPipe;
 

/**
 * The Genbank Pipe takes as input a filename with complete path and streams out
 * BioJava SeqIO objects
 * String - filenames come in
 * BioJava RichSequence comes out
 *  -- a RichSequence is a sequence plus all of it's annotation
 * @author Daniel Quest
 */
public class GenbankPipe extends AbstractPipe<String, RichSequence>{

	/* 
	 * ReadGES_BJ1_6.java - A pretty simple demo program to read a sequence file
	 * with a known format using Biojavax extension found in BJ1.6. 
	 * 
	 * You only need to provide a file as args[0]
	 */
//	public static void main(String[] args) throws IOException {
//		try{
//			BufferedReader br = new BufferedReader(new FileReader("/Users/m102417/workspace/bior_etl/src/test/resources/etl/testData/example.gbk"));
//			SimpleNamespace ns = new SimpleNamespace("biojava");
// 
//			// You can use any of the convenience methods found in the BioJava 1.6 API
//			//RichSequenceIterator rsi = RichSequence.IOTools.readFastaDNA(br,ns);
//                        RichSequenceIterator rsi = RichSequence.IOTools.readGenbankDNA(br, ns);
// 
//			// Since a single file can contain more than a sequence, you need to iterate over
//			// rsi to get the information.
//			while(rsi.hasNext()){
//				RichSequence rs = rsi.nextRichSequence();
//				System.out.println(rs.getName());
//			}
//		}
//		catch(Exception be){
//			be.printStackTrace();
//			System.exit(-1);
//		}
//	}
        
    private BufferedReader openFile(String filename) throws Exception{
        br = br = new BufferedReader(new FileReader(filename));
        ns = new SimpleNamespace("biojava");
        // You can use any of the convenience methods found in the BioJava 1.6 API
        rsi = RichSequence.IOTools.readGenbankDNA(br, ns);
        return br;
    }
        
    private BufferedReader br = null;
    private SimpleNamespace ns = null;
    private RichSequenceIterator rsi = null;

    @Override
    protected RichSequence processNextStart() throws NoSuchElementException {
        if(rsi == null){
            try {
                //System.out.println("***********" + this.starts.next());
                openFile(this.starts.next());
                //openFile("/Users/m102417/workspace/bior_etl/src/test/resources/etl/testData/example.gbk");
            } catch (Exception e) {
            	throw new NoSuchElementException(e.getMessage());
            }
        }
        
    	if(rsi.hasNext()) {
            try {
                return rsi.nextRichSequence();
            } catch (BioException ex) {
                throw new NoSuchElementException(ex.getMessage());
                //Logger.getLogger(GenbankPipe.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            //get from the next file......
            try {
            	openFile(this.starts.next());
                
            	if(rsi.hasNext()){
            		return rsi.nextRichSequence();
                }
            } catch (Exception e) {
            	throw new NoSuchElementException(e.getMessage());
            }
        }
        
    	throw new NoSuchElementException();
    }
}
