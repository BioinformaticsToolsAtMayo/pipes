/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.aggregators.TokenAggregatorPipe;
import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 *
 * @author m102417
 * Fasta Pipe takes in a Fasta formated file (not fastq!) and outputs data as strings formatted as:
 * >header\tsequence
 * 
 */
public class FastaPipe extends AbstractPipe<String, String>{

    ArrayList<String> baggins = new ArrayList<String>();
    
    @Override
    public String processNextStart() throws NoSuchElementException {
        while(this.starts.hasNext()){
            String next = this.starts.next();
            if(next.startsWith(">") && baggins.size()>1){
                String ret = format(baggins);
                baggins.clear();
                baggins.add(next);
                return ret;
            }else {
                baggins.add(next);
            }
        }
        if(baggins.size()>0){
                String ret = format(baggins);
                baggins.clear();
                return ret;
        }
        throw new NoSuchElementException();
    }
    
    public String format(ArrayList<String> baggins){
        StringBuilder sb = new StringBuilder();
        int i=0;
        for(String line : baggins){
            sb.append(line);
            if(i==0){
                sb.append("\t");
            }
            i++;
        }
        return sb.toString();
    }
    
    

//    @Override
//    protected ArrayList<String> processNextStart() throws NoSuchElementException {
//        if(this.starts.hasNext()){
//            ArrayList<String> record = new ArrayList<String>();
//            TokenAggregatorPipe baggins = new TokenAggregatorPipe(">");
//            Pipeline p = new Pipeline(baggins);
//            p.setStarts(this.starts);
//            ArrayList<String> lines = (ArrayList<String>) p.next();
//            for(String line : lines){
//                if(line.startsWith(">")){
//                    record.add(line);//the first line in the record is the header
//                }else{
//                    
//                }
//            }
//            return record;
//        }else{
//            throw new NoSuchElementException();
//        }
//        
//    }
//    
//        StringBuffer sb = null;
//    String linebuffer = null;
//    StringBuffer Sequence;
//    String Header = "";
//    private Graph g;
//    private Vertex fastafile = null;
//    public FastaFilter(Graph graph){
//        g = graph;
//    }
//
//    protected Vertex processNextStart() {
//        Vertex record = null;
//        String line = null;
//        if (!this.starts.hasNext())
//            throw new NoSuchElementException();
//        else {
//            if(fastafile == null){
//                   fastafile  = g.addVertex(null);
//            }
//            if(this.linebuffer != null){
//                line = linebuffer;
//                linebuffer = null;
//            }else{
//                line = this.starts.next();
//            }
//            if(line.startsWith(">")){
//                    record =  g.addVertex(null);
//                    g.addEdge(null, fastafile, record, "Hornet:containsRecord");
//                    Header = line.replace(">","");
//                    Header = Header.replace("\n","");
//                    record.setProperty("Hornet:header", Header);
//                    Sequence = new StringBuffer();
//                    line = this.starts.next();
//            }
//            while(!line.startsWith(">") && line.matches("^[ARNDCEQGHILKMFPSTWYVarndceqghilkmfpstwyvXx]+")){
//                line = line.replace("\n","");
//                Sequence.append(line);
//                line = this.starts.next();
//            }
//            record.setProperty("Hornet:sequence", Sequence.toString());
//            Sequence = null;
//            return record;
//        }
//
//    }
    
}
