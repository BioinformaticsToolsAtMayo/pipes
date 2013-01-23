/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics.sequence;

import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.JSON.tabix.TabixParentPipe;
import edu.mayo.pipes.JSON.tabix.TabixReader;
import edu.mayo.pipes.JSON.tabix.TabixSearchPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.history.History;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author dquest
 * Input is a genomic interval in the format (as an ArrayList)
 * _landmark    _minBP  _maxBP
 * _minBP is 1 based!
 * 
 * The output is:
 * _landmark    _minBP  _maxBP  sequence
 * e.g.
 * 22    1000    1010    agtccagtta
 * _landmark in this example is chr22.
 * 
 * In general, this pipe will ignore any columns beyond the last 3.  e.g.
 * foo  bar baz 22    1000    1010
 * is the same as
 * 22    1000    1010
 * 
 */
public class Bed2SequencePipe extends AbstractPipe<ArrayList<String>,ArrayList<String>> {

    TabixSearchPipe search;
    public Bed2SequencePipe(String tabixDataFile) throws IOException {
        search = new TabixSearchPipe(tabixDataFile);
    }
    
    /**
     * A substring for biological sequences
     * 
     * min is minbp for the interval
     * max is maxbp for the interval
     * start is the start of the genomic sequence tabix extracted
     * end is the end of the genomic sequence tabix extracted
     * 
     * start < min < max < end
     * 
     *    start  min    max      end
     *    |      |      |        |
     *    ========================
     * 
     */
    public String oneBasedSubsequence(String sequence, int min, int max){
        System.out.println(start);
        System.out.println(end);
        System.out.println(min);
        System.out.println(max);
        
        return sequence.substring(min-start, max-start+1);
    }
    

    int start = 0;
    int end = 0;
    String result = "";
    TabixReader.Iterator records;
    @Override
    public ArrayList<String> processNextStart() throws NoSuchElementException {
        try {
            ArrayList<String> al = this.starts.next();
            
            String record;
            int x = new Integer(al.get(al.size()-2));
            int y = new Integer(al.get(al.size()-1));
            String query = al.get(al.size()-3) + ":" + x + "-" + y;
            StringBuilder subsequence = new StringBuilder();
            records = search.tquery(query);
            for(int i=1;(record = records.next()) != null; i++){
                System.out.println(record);
                String[] split = record.split("\t");
                subsequence.append(split[3]);
                if(i==1){                 
                    start = new Integer(split[1]);
                }
                end = new Integer(split[2]);//keep updating it, eventually it will be correct.
            }
            
            al.add( oneBasedSubsequence(subsequence.toString(), x, y) );
            
            return al;
        } catch (IOException ex) {
            throw new NoSuchElementException();//perhaps bad??
        }
    }
    
    
}
