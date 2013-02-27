package edu.mayo.pipes.exceptions;

import com.tinkerpop.pipes.Pipe;

/**
 * Thrown to indicate that the Pipe has attempted to process an input object,
 * but that the input object is invalid.
 * 
 * @author duffp
 * 
 */
public class InvalidPipeInputException extends RuntimeException {

	// serialization
	private static final long serialVersionUID = 1L;

	private Pipe mPipe;

	/**
	 * Constructor
	 * 
	 * @param message
	 *            User friendly message that indicates why the input object is
	 *            invalid.
	 * @param pipe
	 *            The Pipe that is processing the input object.
	 */
	public InvalidPipeInputException(String message, Pipe pipe) {
		super(message);

		mPipe = pipe;
	}

	/**
	 * Gets the Pipe that is processing the input object.
	 */
	public Pipe getPipe() {
		return mPipe;
	}

}
