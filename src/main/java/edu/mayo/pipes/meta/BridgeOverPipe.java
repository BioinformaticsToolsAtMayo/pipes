/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.meta;

import com.tinkerpop.pipes.AbstractPipe;
import com.tinkerpop.pipes.FunctionPipe;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.TransformFunctionPipe;
import com.tinkerpop.pipes.util.MetaPipe;
import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.pipeFunctions.StitchPipeFunction;
import edu.mayo.pipes.sideEffect.CachePipe;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *
 * @author Dan Quest
 */
public class BridgeOverPipe<S,T,E> extends AbstractPipe<S, E> implements MetaPipe {

    private final StitchPipeFunction<T, S, E> mergeFunction;
    private final Pipeline<S,E> pipeline;
    private CachePipe<S> cache;
    
    /**
     * BridgeOverPipe 'supervises' a pipeline provided by the calling function.  
     * The way this works is:
     * 1) it appends a splitFunction as a pipe onto the start of the pipeline
     * 2) it appends a CachePipe at the start of the pipeline. On a call to next(), this will hold a value, X which is whatever went into the pipeline.
     * 3) it instantiates/resets its internal pipeline to be whatever pipeline was passed into the constructor.
     * 4) on a call to next(), it calls next on the pipeline to get a value Y, then returns mergeFunction(Y,X).
     * caches the contents.
     * @param pipeline
     * @param splitFunction
     * @param mergeFunction 
     */
    public BridgeOverPipe(final Pipeline<S,E> pipeline, final PipeFunction splitFunction, final StitchPipeFunction mergeFunction) {
        pipeline.addPipe(0, new TransformFunctionPipe(splitFunction)); //put the split at the second to last position in the pipeline
        cache = new CachePipe<S>();
        pipeline.addPipe(0, cache); //put the CachePipe on the start of the pipeline so we can go back to objects that just passed through the pipeline
        this.pipeline = pipeline; 
        this.mergeFunction = mergeFunction;
    }
    
    /**
     * BridgeOverPipe 'supervises' a pipeline provided by the calling function.  
     * The way this works is:
     * 1) it appends a CachePipe at the start of the pipeline. On a call to next(), this will hold a value, X which is whatever went into the pipeline.
     * 2) it instantiates/resets its internal pipeline to be whatever pipeline was passed into the constructor.
     * 3) on a call to next(), it calls next on the pipeline to get a value Y, then returns mergeFunction(Y,X).
     * caches the contents.
     * @param pipeline
     * @param mergeFunction 
     */
    public BridgeOverPipe(final Pipeline<S,E> pipeline, final StitchPipeFunction mergeFunction) {
        cache = new CachePipe<S>();
        pipeline.addPipe(0, cache); //put the CachePipe on the start of the pipeline so we can go back to objects that just passed through the pipeline
        this.pipeline = pipeline; 
        this.mergeFunction = mergeFunction;
    }
    
    @Override
    protected E processNextStart() throws NoSuchElementException {
        this.pipeline.reset();
        this.pipeline.setStarts(Arrays.asList( this.starts.next() ));
        final T y = (T) this.pipeline.next();
        final S x = cache.get();
        return mergeFunction.compute(y, x);
    }

    @Override
    public List<Pipe> getPipes() {
        return this.pipeline.getPipes();
    }
    
    public Pipeline getPipeline(){
        return this.pipeline;
    }
    
}
