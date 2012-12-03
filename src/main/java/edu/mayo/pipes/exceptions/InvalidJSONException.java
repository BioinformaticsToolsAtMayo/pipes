/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.exceptions;

/**
 *
 * @author m102417
 * Use this exception when we are expecting JSON, but the JSON is incorrect.
 */
public class InvalidJSONException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public InvalidJSONException (String arg0, Throwable arg1) {
		super(arg0, arg1);
	}
	
	public InvalidJSONException (String arg0) {
		super(arg0);
	}
	
	public InvalidJSONException (Throwable arg0) {
		super(arg0);
	}
}
