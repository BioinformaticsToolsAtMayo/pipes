/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics.alignment;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.biojava.bio.BioException;
import org.biojava.bio.alignment.AlignmentPair;
import org.biojava.bio.alignment.NeedlemanWunsch;
import org.biojava.bio.alignment.SubstitutionMatrix;
import org.biojava.bio.seq.DNATools;

import org.biojava.bio.seq.ProteinTools;
import org.biojava.bio.seq.Sequence;
import org.biojava.bio.symbol.AlphabetManager;
import org.biojava.bio.symbol.FiniteAlphabet;
import org.biojava.bio.symbol.IllegalSymbolException;
import org.biojava.bio.symbol.Symbol;


public class AlgorithmExample {



    public static void main(String[] args) throws IllegalSymbolException, BioException, NumberFormatException, IOException {
        AlgorithmExample test = new AlgorithmExample();
        //test.testrunAA();
        test.testrunDNA();
    }
  
    
    private final short GAPOPEN = 2;
    private final short GAPEXTEND = 2;
    private final short MATCH = 1;
    private final short MISMATCH = -1;
    
    public void testrunDNA() throws IllegalSymbolException, BioException, NumberFormatException, IOException{
        Sequence query  = DNATools.createDNASequence("GCTGA", "query");
        Sequence target = DNATools.createDNASequence("GCCTGA", "target");
        FiniteAlphabet alphabet = (FiniteAlphabet) AlphabetManager.alphabetForName("DNA");
        SubstitutionMatrix matrix = new SubstitutionMatrix(alphabet, new File("src/main/resources/blosum62NA.mat"));
        NeedlemanWunsch aligner = new NeedlemanWunsch(MATCH, MISMATCH, GAPOPEN, GAPOPEN, GAPEXTEND, matrix);
        AlignmentPair pair = aligner.pairwiseAlignment(query, target);
			System.out.println(pair.formatOutput(100));
            
                       
        System.out.println("SeqString: " + pair.seqString());
        Iterator<Symbol> i = pair.iterator();
        while(i.hasNext()){
            System.out.println(i.next());
        }
        int correct = pair.getNumIdenticals() + pair.getNumSimilars();
        System.out.println("Correct: " + correct);
        System.out.println("Gap Query: " +pair.getNumGapsInQuery());
        System.out.println("Gap Subject: " +pair.getNumGapsInSubject());
        int gaps = pair.getNumGapsInQuery() + pair.getNumGapsInSubject();
        System.out.println("Gaps: " + gaps);
        int error = query.length() + target.length() - 2*pair.getNumIdenticals();
        System.out.println("Error: " +  error);
    }
    
    private final short PGAPOPEN = 10;
    private final short PGAPEXTEND = 5;
    private final short PMATCH = 0;
    private final short PMISMATCH = 0;

    public void testrunAA() {
        try {
            String header1 = ">gb|CP001821.1|:690821-692026 translation elongation factor Tu [Xylanimonas cellulosilytica DSM 15894]";
            String protein1 = "VAKAKFERTKPHVNVGTIGHVDHGKTTLTAAISKTLAEKYPASEGYLANQVVDFDGIDKAPEEKQRGITINISHIEYETPNRHYAHVDAPGHADYIKNMITGAAQMDGAILVVAATDGPMAQTREHVLLARQVGVPYLLVALNKSDMVDDEEILELVEMEVRELLSSQGFDGDDAPVVRVSGLKALEGDPEWQAKVLELMEAVDTNVPEPVRDLDKPFLMPIEDVFTITGRGTVVTGKVERGALNVNSEVEIVGIRNPQKTTVTGIETFHKSMDQAQAGDNTGLLLRGIKREDVERGQVVVKPGSITPHTDFEAQVYILGKDEGGRHNPFYSNYRPQFYFRTTDVTGVISLPEGTEMVMPGDNTEMTVELIQPIAMEEGLGFAIREGGRTVGSGRVTKIIK";
            String header2 = ">dbj|AP006618.1|:c5375075-5373828 putative translation elongation factor TU [Nocardia farcinica IFM 10152]";
            String protein2 = "MTPRTAATAGTNTVQEDKTVAKAKFERTKPHVNIGTIGHVDHGKTTLTAAITKVLADKYPDLNQSFAFDQIDKAPEEKARGITINISHVEYQTEKRHYAHVDAPGHADYIKNMITGAAQMDGAILVVAATDGPMPQTREHVLLARQVGVPYILVALNKADMVDDEEILELVEMEVRELLAAQEFDEEAPVVRVSGLKALEGDPKWVKSVEDLMDAVDESIPDPVRETDKPFLMPIEDVFTITGRGTVVTGRVERGIINVNEEVEITGIRPETTKTTVTGIEMFRKLLDQGQAGDNVGLLIRGIKREDVERGQVVIKPGTTTPHTEFEGQAYILSKDEGGRHTPFFNNYRPQFYFRTTDVTGVVTLPEGTEMVMPGDNTEMSVKLIQPVAMEEGLRFAIREGGRTVGAGRVTKIIK";
            Sequence query = ProteinTools.createProteinSequence(protein1, header1.split("\\|")[1]);
            Sequence target = ProteinTools.createProteinSequence(protein2, header2.split("\\|")[1]);
            FiniteAlphabet alphabet = (FiniteAlphabet) AlphabetManager.alphabetForName("PROTEIN-TERM");
            SubstitutionMatrix matrix = new SubstitutionMatrix(alphabet, new File("src/main/resources/blosum62.mat"));
            NeedlemanWunsch aligner = new NeedlemanWunsch(PMATCH, PMISMATCH, PGAPOPEN, PGAPOPEN, PGAPEXTEND, matrix);
//            Alignment alignment = aligner.getAlignment(query, target);
//            System.out.println(aligner.getAlignmentString());
            AlignmentPair pair = aligner.pairwiseAlignment(query, target);
			System.out.println(pair.formatOutput(100));
            System.out.printf("\n%d\t%d\n", protein1.length(), protein2.length());
        } catch (Exception ex) {
            Logger.getLogger(AlgorithmExample.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }
}