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
    
    public static String toRegEX(String delim){
        if(delim.equals(PIPE) || delim.equals("|")){
            return "\\|";
        }if(delim.equals(TAB)){
            return "\\t";
        }if(delim.equals(COMMA)){
            return ",";
        }if(delim.equals(SEMICOLON)){
            return ";";
        }if(delim.equals(COLON)){
            return ":";
        }if(delim.equals(PERIOD)){
            return "\\.";
        }if(delim.equals(EQUAL)){
            return "=";
        }if(delim.equals(SPACE)){
            return " +";
        }else {
            return delim;
        }
    }
    
}
