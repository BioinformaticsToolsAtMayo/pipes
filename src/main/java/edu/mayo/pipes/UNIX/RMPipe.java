/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.UNIX;

import com.tinkerpop.pipes.AbstractPipe;
import java.io.File;

/**
 *
 * @author Removes files as they go through the pipe
 */
public class RMPipe extends AbstractPipe<String, String> {
  public String processNextStart() {
    String filePathString = this.starts.next();
    File f = new File(filePathString);
    if(f.exists()) { f.delete(); }
    return filePathString;
  }
}