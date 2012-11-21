/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.HashMap;

/**
 *
 * @author m102417
 */
public class JSONUtil {
    
    public static JsonArray stringArr2JSON(String[] s){
        JsonArray jarr = new JsonArray();
        for(int i=0;i<s.length;i++){
            JsonObject obj = new JsonObject();
            obj.addProperty(String.valueOf(i), s[0]);
            jarr.add(obj);
        }

        return jarr;
    }
    
    public static JsonObject stringHash2JSON(HashMap<String,String> hm){
        JsonObject jobj = new JsonObject();
        for(String key : hm.keySet()){
            jobj.addProperty(key, (String)hm.get(key));
        }
        return jobj;
    }
    
}
