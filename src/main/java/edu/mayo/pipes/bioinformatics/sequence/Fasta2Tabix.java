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
import edu.mayo.pipes.util.GenomicObjectUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

    List<String> landmarks = Arrays.asList(
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
            "16",
            "17",
            "18",
            "19",
            "20",
            "21",
            "22",
            "X",
            "Y",
            "MT"
            );

    /**
     *
     * @param dir  - the directory where all the source data is
     * @return list of paths for the data we need to process
     */
    public List<String> createRefFileList(String dir){
        List<String> paths = new ArrayList<String>();
        for(String l : landmarks){
            paths.add(dir + "hs_ref_GRCh37.p10_chr" + l +".fa.gz");
        }
        return paths;
    }


    public void processDir(String dir, String output){
        List<String> paths = createRefFileList(dir);
        int i = 0;
        for(String path : paths){
            process(path,output,landmarks.get(i));
            i++;
        }

    }

    /**
     * process will append the raw data from the genome file onto the output file
     * @param inputFile
     * @param outputFile
     */
    public void process(String inputFile, String outputFile, String landmark){
        String chr = GenomicObjectUtils.computechr(landmark);
        System.out.println("Opening File: " + inputFile);
        System.out.println("Using Landmark: " + landmark);
        System.out.println("Writing File: " + outputFile);
        Pipe<String,String> t = new TransformFunctionPipe<String,String>( new Fasta2SequenceTabix(chr) );
        Pipe p = new Pipeline(new CatGZPipe("gzip"),
                new HeaderPipe(1), //don't want to grep out header >, that would take too long!
                t,
                new WritePipe(outputFile, true)
                //new PrintPipe()
        );
        p.setStarts(Arrays.asList(inputFile));
        for(int i=0; p.hasNext(); i++){
            p.next();
//            if(i>1000)
//                break;

        }
        return;

    }
    
    public static void main(String[] args){
        Fasta2Tabix f2t = new Fasta2Tabix();
        String dir = "/data/NCBIgene/genomes/H_sapiens/Assembled_chromosomes/seq/";
        String out = "/tmp/hs_ref_GRCh37.p10.fa.tsv";
        f2t.processDir(dir, out);
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
