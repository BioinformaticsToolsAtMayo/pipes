/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.security.*;

/**
 *
 * @author m102417
 */
public class MD5Util {
    

    public String getmd5(String contents){
        //String sessionid="12345";

    byte[] defaultBytes = contents.getBytes();
    try{
        MessageDigest algorithm = MessageDigest.getInstance("MD5");
        algorithm.reset();
        algorithm.update(defaultBytes);
        byte messageDigest[] = algorithm.digest();

        StringBuffer hexString = new StringBuffer();
        for (int i=0;i<messageDigest.length;i++) {
            hexString.append(Integer.toHexString(0xFF & messageDigest[i]));
        }
        //String foo = messageDigest.toString();
        //System.out.println("sessionid "+sessionid+" md5 version is "+hexString.toString());
        //sessionid=hexString+"";
        return hexString.toString();
    }catch(NoSuchAlgorithmException nsae){

    }
       return null;
    }


}