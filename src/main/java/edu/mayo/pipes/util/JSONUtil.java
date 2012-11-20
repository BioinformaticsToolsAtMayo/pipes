/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 *
 * @author m102417
 */
public class JSONUtil {
    
    public JsonArray stringArr2JSON(String[] s){
        JsonArray jarr = new JsonArray();
        for(int i=0;i<s.length;i++){
            JsonObject obj = new JsonObject();
            obj.addProperty(String.valueOf(i), s[0]);
            jarr.add(obj);
        }

        return jarr;
    }
    
}
