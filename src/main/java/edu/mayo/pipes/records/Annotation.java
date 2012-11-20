/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.records;

/**
 *
 * @author m102417
 */
public class Annotation {
    private String id;  //the uniqueID for the project e.g. accessionID if availible
    private String type;
    /** this is the data blob/payload */
    private String json;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
    
}
