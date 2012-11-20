/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import com.tinkerpop.pipes.AbstractPipe;
import java.util.*;
import org.biojava.bio.seq.Feature;
import org.biojava.bio.seq.FeatureFilter;
import org.biojava.bio.seq.FeatureHolder;
import org.biojavax.*;
import org.biojavax.bio.seq.RichFeature;
import org.biojavax.bio.seq.RichSequence;
import org.biojavax.ontology.ComparableTerm;
import com.tinkerpop.pipes.AbstractPipe;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.biojavax.bio.seq.RichSequence;
//Java libraries
import java.io.*;
import java.util.*;
//BioJava libraries
import org.biojava.bio.*;
import org.biojava.bio.seq.*;
import org.biojava.bio.seq.io.*;
//BioJava extension libraries
import org.biojavax.*;
import org.biojavax.ontology.*;
import org.biojavax.bio.*;
import org.biojavax.bio.seq.*;

/**
 *
 * @author m102417
 */
public class BioJavaRichSequence2JSON extends AbstractPipe<RichSequence, List<String>>{
    
    private String chromosome;
    public BioJavaRichSequence2JSON(String chr){
        this.chromosome = chr;
    }

    //Use BioJava defined ComparableTerms 
    public ComparableTerm geneTerm = new RichSequence.Terms().getGeneNameTerm();
    public ComparableTerm synonymTerm = new RichSequence.Terms().getGeneSynonymTerm();
    //Create the required additional ComparableTerms
    public ComparableTerm locusTerm = RichObjectFactory.getDefaultOntology().getOrCreateTerm("locus_tag");
    public ComparableTerm productTerm = RichObjectFactory.getDefaultOntology().getOrCreateTerm("product");
    public ComparableTerm proteinIDTerm = RichObjectFactory.getDefaultOntology().getOrCreateTerm("protein_id");
    public ComparableTerm noteTerm = RichObjectFactory.getDefaultOntology().getOrCreateTerm("note");

    @Override
    protected List<String> processNextStart() throws NoSuchElementException {
        
        if(this.starts.hasNext()){
            
            RichSequence seq = this.starts.next();
            //TODO: this needs to be filled in
            List<String> genes = null;//transform(seq);
            return genes;
        }else{
            throw new NoSuchElementException();
        }
    }

/*    
    private List<String> transform(RichSequence rs){    
    	System.out.println("BioJavaRichSequence2ThriftGenes.transform()..");
        List<String> genes = new ArrayList<Gene>();
        //Filter the sequence on CDS features
        FeatureFilter ff = new FeatureFilter.ByType("gene");//CDS
        FeatureHolder fh = rs.filter(ff);
 
        //Iterate through the gene features
        for (Iterator <Feature> i = fh.features(); i.hasNext();){
            RichFeature rf = (RichFeature)i.next();
            Gene g = new Gene();
            g.setNspace(nspace);
            g.setVersion(version);
            
            g.setChr(this.chromosome);

            //Get the strand orientation of the feature
            char featureStrand = rf.getStrand().getToken();
            if(featureStrand == '+'){
                g.setStrand(Strand.FORWARD);
            }else if(featureStrand == '-'){
                g.setStrand(Strand.REVERSE);
            }else {
                g.setStrand(Strand.UNSPECIFIED);
            }           

            //Get the location of the feature
            String featureLocation = rf.getLocation().toString(); 
            g.setMinBP(rf.getLocation().getMin());
            g.setMaxBP(rf.getLocation().getMax());
            //System.out.println(featureLocation);

            //Get the annotation of the feature
            RichAnnotation ra = (RichAnnotation)rf.getAnnotation(); 


            //Iterate through the notes/qualifiers in the annotation 
            for (Iterator <Note> it = ra.getNoteSet().iterator(); it.hasNext();){
                Note note = it.next();

                //System.out.println(note.getTerm() + ":*:" + note.getValue());

                //Check each note to see if it matches one of the required ComparableTerms
                if(note.getTerm().equals(locusTerm)){
                    String locus = note.getValue().toString();
                    g.setId(locus);
                }else if(note.getTerm().equals(productTerm)){
                    String product = note.getValue().toString();
                }else if(note.getTerm().equals(geneTerm)){
                    String genestr = note.getValue().toString();
                    System.out.println("BioJavaRichSequence2ThriftGenes:HgncSymbol="+genestr);
                    g.setHgncSymbol(genestr);
                }else if(note.getTerm().equals(synonymTerm)){
                    String geneSynonym = note.getValue().toString();
                }else if(note.getTerm().equals(noteTerm)){
                    String n = note.getValue().toString();
                    g.setDescription(n);
                }else {
                    g.putToProperties(note.getTerm().toString(), note.getValue().toString());
                }
            }

            //Get the dbxrefs...
            Set<RankedCrossRef> rankedCrossRefs = rf.getRankedCrossRefs();
            for (Iterator <RankedCrossRef> it = rankedCrossRefs.iterator(); it.hasNext();){
                RankedCrossRef next = it.next();
                CrossRef crossRef = next.getCrossRef();
                //System.out.println(crossRef.getDbname() + ":*:" + crossRef.getAccession());
                if(crossRef.getDbname().equals("GeneID")){
                    g.setId(crossRef.getAccession());
                }else if(crossRef.getDbname().equals("MIM")){
                    g.setOmimID(crossRef.getAccession());
                }else if(crossRef.getDbname().equals("HGNC")){
                    g.setHgncID(crossRef.getAccession());
                }else {
                    g.putToGxrefs(crossRef.getDbname(), crossRef.getAccession());
                }
            }

            //Add the current gene to the list of genes
            genes.add(g);
            //System.out.println(g);
        }
        return null;
    }
    * 
    */
    
}
