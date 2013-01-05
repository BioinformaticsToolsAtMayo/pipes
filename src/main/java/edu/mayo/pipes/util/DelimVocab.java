/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.util;

/**
 *
 * @author m102417
 */
public class DelimVocab {
    //delimiters...not in use yet
    public static final String COMMA = "comma";
    public static final String PIPE = "pipe";
    public static final String SEMICOLON = "semicolon";
    public static final String COLON = "colon";
    public static final String EQUAL = "equal";
    public static final String TAB = "tab";
    public static final String PERIOD = "period";
    public static final String SPACE = "space";
    
    /** Convert a typical delimiter character to a version suitable 
     *  for String.split() operation (which uses Regular Expressions) */
    public static String toRegEX(String delim){
        if(delim.equals(PIPE) || "|".equals(delim)){
            return "\\|";
        }else if(delim.equals(TAB)){
            return "\t";
        }else if(delim.equals(COMMA)){
            return ",";
        }else if(delim.equals(SEMICOLON)){
            return ";";
        }else if(delim.equals(COLON)){
            return ":";
        }else if(delim.equals(PERIOD) || ".".equals(delim)){
            return "\\.";
        }else if(delim.equals(EQUAL)){
            return "=";
        }else if(delim.equals(SPACE) || " ".equals(delim)){
            return " +";
        }else {
            return delim;
        }
    }
    
}
