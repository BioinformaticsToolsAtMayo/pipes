/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
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
import org.biojava.bio.symbol.Location;
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
    private String[] featureTypes;
    public BioJavaRichSequence2JSON(String chr, String[] featureTypes){
        this.chromosome = chr;
        this.featureTypes = featureTypes;
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
            List<String> features = transform(seq);
            
            return features;
        }else{
            throw new NoSuchElementException();
        }
    }
   
    private List<String> transform(RichSequence rs) throws NoSuchElementException {    
    	System.out.println("BioJavaRichSequence2ThriftGenes.transform()..");
        List<String> features = new ArrayList<String>();
        
        for(int i = 0; i<featureTypes.length; i++){
            List<String> extFeatures = extractFeaturesByType(featureTypes[i], rs);
            features.addAll(extFeatures);   
        }
        
        if(features.size() < 1 ){
            throw new NoSuchElementException();
        }
        
        return features;
    }
    
    /**
     * Extract all features of a given type (e.g. gene, CDS, exon, mRNA, ...)
     * convert those featues to a JSON String and return a list of the converted strings
     * @param type
     * @return 
     */
    private List<String> extractFeaturesByType(String type, RichSequence rs){
        List<String> features = new ArrayList<String>();
        
        //Filter the sequence on CDS,gene, mRNA or whatever type of features
        FeatureFilter ff = new FeatureFilter.ByType(type);//CDS
        FeatureHolder fh = rs.filter(ff);
        
        //Iterate through the features
        for (Iterator <Feature> i = fh.features(); i.hasNext();){
            RichFeature rf = (RichFeature)i.next();
            JsonObject f = new JsonObject(); //the next feature
            f.addProperty("type", type);
            f.addProperty("chr", this.chromosome);                     
            
            //Get the strand orientation of the feature
            char featureStrand = rf.getStrand().getToken();
            if(featureStrand == '+'){
                f.addProperty("strand", "+");
            }else if(featureStrand == '-'){
                f.addProperty("strand", "-");
            }else {
                f.addProperty("strand", ".");
            }
            
            String subregionType = "subregions";
            JsonArray subregions = new JsonArray();
            if(type.equalsIgnoreCase("mRNA") || type.equalsIgnoreCase("transcript") || type.equalsIgnoreCase("CDS")){
                subregionType = "exons";
            }
            
            //exons : [123,234],[345,456],... 
            
            //Get the location of the feature
            String featureLocation = rf.getLocation().toString(); 
            f.addProperty("minBP", rf.getLocation().getMin());
            f.addProperty("maxBP", rf.getLocation().getMax());
            //System.out.println(featureLocation);
            
            //Some Features have many locations joined together (e.g. a transcript consists of multiple exons)
            Location location = rf.getLocation();
            Iterator<Location> blockIterator = location.blockIterator();
            while(blockIterator.hasNext()){
                JsonObject sregion = new JsonObject();
                Location next = blockIterator.next();
                int min = next.getMin();
                int max = next.getMax();              
                //System.out.println("Pos =" + min + ":" + max);
                sregion.addProperty("minBP", min);
                sregion.addProperty("maxBP", max);
                subregions.add(sregion);
            }
            if(subregions.size() > 1){
                f.add(subregionType, subregions);
            }else if(type.equalsIgnoreCase("mRNA") || type.equalsIgnoreCase("transcript") || type.equalsIgnoreCase("CDS")){
                f.add(subregionType, subregions);
            }
            

            //Get the annotation of the feature
            RichAnnotation ra = (RichAnnotation)rf.getAnnotation();
                       
            //Iterate through the notes/qualifiers in the annotation 
            for (Iterator <Note> it = ra.getNoteSet().iterator(); it.hasNext();){
                Note note = it.next();
                f.addProperty(note.getTerm().toString().replace("biojavax:", ""), note.getValue().toString());
            }
            
            //Get the dbxrefs...
            Set<RankedCrossRef> rankedCrossRefs = rf.getRankedCrossRefs();
            for (Iterator <RankedCrossRef> it = rankedCrossRefs.iterator(); it.hasNext();){
                RankedCrossRef next = it.next();
                CrossRef crossRef = next.getCrossRef();
                f.addProperty(crossRef.getDbname(), crossRef.getAccession());
            }
            
            features.add(f.toString());   
        }
        
        
        return features;
    }

    
}
