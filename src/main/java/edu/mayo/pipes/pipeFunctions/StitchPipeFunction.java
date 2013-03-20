/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.pipeFunctions;

/**
 *
 * A gets merged with B and C is the result.
 * 
 * T - the type of variable that comes into the Pipe at the end of some other pipe e.g. List<String>
 * S - the type of variable that was cached (original) and its data is now being stitched together with T.
 * E - the type returned from the stitch between data flowing through the pipeline (type T) and 
 * data cached or injected (type S)
 * @author m102417
 */
public interface StitchPipeFunction<T, S, E> {

    /**
     * A function that takes an arguments of type S and T and returns a result of type E.
     * S - Start
     * T - Transformed
     * E - End
     *
     * @param arg1 An argument of type T
     * @param arg2 An argument of type S
     * @return the result of computing the function on the argument
     */
    public E compute(T arg1, S arg2);
}
