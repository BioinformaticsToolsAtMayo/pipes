/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.util;

import edu.mayo.pipes.bioinformatics.vocab.Undefined;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 *
 * @author m102417
 */
public class GenomicObjectUtils {
    public static final String DELIMTOKEN = ";#__#;";
    public static char getStrand(String s){
        if(s.contains("+")) return '+';
        if(s.contains("-")) return '-';
        else return '.';
    }
    
    public static String stringifyList(List<String> l){
        java.util.Collections.sort(l);
        StringBuilder sb = new StringBuilder();
        for(String ele : l){
            sb.append(ele);
            sb.append(DELIMTOKEN);
        }
        return sb.toString();
    }
    
    public static List<String> listifyString(String s){
        return Arrays.asList(s.split(DELIMTOKEN));
    }
    
    public static char computeStrandFromNumbers(int strand){
    	return computeStrandFromNumbers(new Integer(strand));
    }
    public static char computeStrandFromNumbers(String strand){
    	if(strand.equalsIgnoreCase("-1")){
        	return '-';
        }else if(strand.equalsIgnoreCase("-1")){
        	return '+';
        }else {
        	return '.';
        }
    }
    
    
    public static String computechr(String raw){
        if(raw.contains("X") || raw.contains("x") || raw.contains("23") ){
            return "X";
        }else if(raw.contains("Y")||raw.contains("y")||raw.contains("24")){
            return "Y";
        }else if(raw.contains("M") || raw.contains("m") || raw.contains("25")  ){
            return "M";
        }else if(raw.contains("22") ){
            return "22";
        }else if(raw.contains("21")){
            return "21";
        }else if(raw.contains("20") ){
            return "20";
        }else if(raw.contains("19") ){
            return "19";
        }else if(raw.contains("18") ){
            return "18";
        }else if(raw.contains("17") ){
            return "17";
        }else if(raw.contains("16") ){
            return "16";
        }else if(raw.contains("15")){
            return "15";
        }else if(raw.contains("14") ){
            return "14";
        }else if(raw.contains("13") ){
            return "13";
        }else if(raw.contains("12") ){
            return "12";
        }else if(raw.contains("11") ){
            return "11";
        }else if(raw.contains("10") ){
            return "10";
        }else if(raw.contains("9") ){
            return "9";
        }else if(raw.contains("8") ){
            return "8";
        }else if(raw.contains("7") ){
            return "7";
        }else if(raw.contains("6") ){
            return "6";
        }else if(raw.contains("5") ){
            return "5";
        }else if(raw.contains("4") ){
            return "4";
        }else if(raw.contains("3") ){
            return "3";
        }else if(raw.contains("2") ){
            return "2";
        }else if(raw.contains("1") ){
            return "1";
        }
        return Undefined.UNKNOWN.toString();
    }


    /**
     * Stores the given String into a Blob.  The String is GZIP'ed to save space.
     * 
     * @see java.sql.Blob
     * 
     * @param conn JDBC connection used to create the vendor specific Blob impl
     * @param str The String to be stored in the Blob.
     * @return Blob containing the GZIP'ed string.
     * @throws SQLException
     * @throws IOException
     */
    /*
    public static Blob toBlob(Connection conn, String str) throws SQLException, IOException {
    	Blob b = conn.createBlob();
		OutputStream outStream = b.setBinaryStream(1);

		GZIPOutputStream gzip = new GZIPOutputStream(outStream);
		gzip.write(str.getBytes("UTF-8"));
		gzip.flush();
		gzip.close();
		
		return b;
    }
    */
}
