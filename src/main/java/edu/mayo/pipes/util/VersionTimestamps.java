/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.util;

import java.io.File;
import java.util.Date;

/**
 *  Basic utility class to get version information into objects consistently.
 * @author m102417
 */
public class VersionTimestamps {    
    public static String dateOfFile(String filenamepath){
        File file = new File(filenamepath);
        Long lastModified = file.lastModified();
        Date date = new Date(lastModified);
        return date.toString();
    }       
}
