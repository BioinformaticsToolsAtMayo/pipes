/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.records;

/**
 *
 * @author m102417
 */
public class Variant extends GenomicFeature {
    private String refAllele;
    private String[] altAllele;

    public String[] getAltAllele() {
        return altAllele;
    }

    public void setAltAllele(String[] altAllele) {
        this.altAllele = altAllele;
    }

    public String getRefAllele() {
        return refAllele;
    }

    public void setRefAllele(String refAllele) {
        this.refAllele = refAllele;
    }
    
}
