/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.records;

/**
 *
 * @author m102417
 */
public class GenomicFeature extends Annotation {
    public int minBP;  //-1 means we don't know
    public int maxBP;
    /** chr in most cases means chromosome, but sometimes could mean source DNA sequence that the feature is on */
    public String chr;     
    /** +,-, or . if we don't know */
    public char strand;

    public String getChr() {
        return chr;
    }

    public void setChr(String chr) {
        this.chr = chr;
    }

    public int getMaxBP() {
        return maxBP;
    }

    public void setMaxBP(int maxBP) {
        this.maxBP = maxBP;
    }

    public int getMinBP() {
        return minBP;
    }

    public void setMinBP(int minBP) {
        this.minBP = minBP;
    }

    public char getStrand() {
        return strand;
    }

    public void setStrand(char strand) {
        this.strand = strand;
    }
    
}
