/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics.sequence;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.TransformFunctionPipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.HeaderPipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.UNIX.CatGZPipe;
import edu.mayo.pipes.UNIX.GrepEPipe;
import edu.mayo.pipes.WritePipe;
import java.util.Arrays;

/**
 * This is a one off utility, that can be used to take a genome/chr in fasta
 * format and format it like this:
 * 1	1	70	NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN
   1	71	140	NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN
   ...
   1	69931	70000	ACACTGAGGAACAAAGACATGAAGACGGCAATAAGACAGCTGAGAAAATGGGATGCACATTCTAGTGTAA
   1	70001	70070	AGTTTTAGATCTTATATAACTGTGAGATTAATCTCAGATAATGACACAAAATATAGTGAAGTTGGTAAGT
   1	70071	70140	TATTTAGTAAAGCTCATGAAAATTGTGCCCTCCATTCCCATATAATTTAGTAATTGTCTAGGAACTTCCA
   ...
   * 
   * to make the catalog, you may need to do something like:
   *   578  bgzip chr22.fa.tsv 
  579  tabix -s 1 -b 2 -e 3 chr22.fa.tsv.bgz 
  580  mv chr22.fa.tsv.gz chr22.fa.tsv.bgz
  581  tabix -s 1 -b 2 -e 3 chr22.fa.tsv.bgz 
 * @author m102417
 */
public class Fasta2Tabix {
    
    public static void main(String[] args){
        String landmark = "Y";
        //String fasta = "src/test/resources/testData/hs_ref_GRCh37.p10_chr"+landmark+".fa.gz";
        String fasta = "/data/NCBIgene/genomes/H_sapiens/Assembled_chromosomes/seq/hs_ref_GRCh37.p10_chr"+landmark+".fa.gz";
        //String out = "/tmp/chr22.fa.tsv";
        String out = "/tmp/hs_ref_GRCh37.p10.fa.tsv";
        System.out.println("Opening File: " + fasta);
        System.out.println("Using Landmark: " + landmark);
        System.out.println("Writing File: " + out);
        Pipe<String,String> t = new TransformFunctionPipe<String,String>( new Fasta2SequenceTabix(landmark) );
                Pipe p = new Pipeline(new CatGZPipe("gzip"), 
                            new HeaderPipe(1), //don't want to grep out header >, that would take too long!
                              t,
                              new WritePipe(out, true)
                              //new PrintPipe()
                        );
        p.setStarts(Arrays.asList(fasta));
        for(int i=0; p.hasNext(); i++){
            p.next();
//            if(i>1000)
//                break;
            
        }
        return;
        
    }
    
    public static class Fasta2SequenceTabix implements PipeFunction<String,String>{

        private String landmark = "";
        private int count = 1;
        public Fasta2SequenceTabix(String landmark){
            this.landmark = landmark;
        }
        
        
        @Override
        public String compute(String s) {
            StringBuilder sb = new StringBuilder();
            if(count>10){
                count++;//not the first line
            }
            sb.append(landmark);
            sb.append("\t");
            sb.append(count);
            sb.append("\t");
            count+=s.length()-1;
            sb.append(count);            
            sb.append("\t");
            sb.append(s);
            sb.append("\n");
            return sb.toString();        
        }
        
    }


    
}
