/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics.tabix;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.ExecPipe;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 *  Make sure you have tabix installed and in your path before you use this pipe!
 * 
 * @author m102417
 * The input to this pipe is a filename.  This file contains in sudo gff format
 * something that looks like this: bior.gff
 * chr1    .       Deletion        10144   10145           .       .       .       {"id":"rs144773400_10144_1","nspace":"dbSNP","version":"137","type":"Deletion","chr":"chr1","minBP":10144,"maxBP":10145,"refAlleleFWD":"TA","altAlleleFWD":["T"],"qual":".","filter":".","rsID":"rs144773400","properties":{"SAO":"0","RSPOS":"10145","OTHERKG":"1","VC":"DIV","VP":"050000000005000002000200","WGT":"1","dbSNPBuildID":"134","ASP":"1","SSR":"0"},"__isset_bit_vector":[1,1],"optionals":["TYPE","CHR","MIN_BP","MAX_BP","STRAND","REF_ALLELE_FWD","ALT_ALLELE_FWD","QUAL","FILTER","ONE_LINER","RS_ID","TAG","DESCRIPTION","AF","ALLELE_COUNTS","PROPERTIES"]}
 * chr1    .       SNP     10177   10177           .       .       .       {"id":"rs201752861_10177_1","nspace":"dbSNP","version":"137","type":"SNP","chr":"chr1","minBP":10177,"maxBP":10177,"refAlleleFWD":"A","altAlleleFWD":["C"],"qual":".","filter":".","rsID":"rs201752861","properties":{"SAO":"0","RSPOS":"10177","OTHERKG":"1","VC":"SNV","VP":"050000000005000002000100","WGT":"1","dbSNPBuildID":"137","ASP":"1","SSR":"0"},"__isset_bit_vector":[1,1],"optionals":["TYPE","CHR","MIN_BP","MAX_BP","STRAND","REF_ALLELE_FWD","ALT_ALLELE_FWD","QUAL","FILTER","ONE_LINER","RS_ID","TAG","DESCRIPTION","AF","ALLELE_COUNTS","PROPERTIES"]}
 * ...
 * 
 * The pipe then executes follows the steps:
 * 1) (grep ^"#" bior.gff; grep -v ^"#" example.bior | sort -k1,1 -k4,4n) | bgzip > sorted.gff.gz
 * 2) tabix -f -p gff sorted.gff.gz ;
 * 
 * The file can then be read at the command line like:
 * - tabix sorted.gff.gz chr1:20000-20245
 * 
 * The TabixReader.java class in this package also has access functions.
 * 
 * input: filename of raw data that needs to be converted to Tabix
 * output: filename of the tabix file.
 * 
 */
public class CreateTabixFilePipe extends AbstractPipe<String, String>{
    private String appendString = ".sort.gz";
    private String grepSortPart1 = "(grep ^\"#\" bior.gff; grep -v ^\"#\" ";
    private String grepSortPart2 = " | sort -k1,1 -k4,4n) | bgzip > ";
    private String tabixCmd = " tabix -f -p gff ";

    @Override
    protected String processNextStart() throws NoSuchElementException {
        if(this.starts.hasNext()){
            String filename = this.starts.next();
            String sortCompress = grepSortPart1 + filename + grepSortPart2;
            Pipe gspipe  = new Pipeline(new ExecPipe());
            System.out.println("Running Command: " + sortCompress);
            gspipe.setStarts(Arrays.asList(sortCompress));
            gspipe.next();
            
            return filename + ".tbi";
        }else {
            throw new NoSuchElementException();
        }
    }
    
}
