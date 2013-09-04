package edu.mayo.pipes.thread;

import com.tinkerpop.pipes.Pipe;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: m102417
 * Date: 9/2/13
 * Time: 9:44 PM
 *
 *
 *  A Filter *consumes* from either a Producer or another Filter,
 *  and *sends* data to either a Consumer or another Filter
 *  So a Filter sits in the middle of a threaded pipeline.
 *
 */
public class Filter implements Runnable {

    protected BlockingQueue inqueue = null;
    protected BlockingQueue outqueue = null;
    private Pipe p;
    private ThreadedPipeline tpipeline = null;
    private boolean run = true;

    /**
     *
     * @param in  the input queue
     * @param out the output queue
     * @param pipe some pipe that performs logic on the input
     */
    public Filter(BlockingQueue in, BlockingQueue out, Pipe pipe, ThreadedPipeline threadedPipeline) {
        inqueue = in;
        outqueue = out;
        p = pipe;
        tpipeline = threadedPipeline;
    }

    public void run() {
        try {
            if(p!=null){
                Object o = inqueue.take();
                p.setStarts(Arrays.asList(o));
                while(o != null && run){
                    if(!p.hasNext()){
                        o = inqueue.take();
                        p.setStarts(Arrays.asList(o));
                        if(!p.hasNext()){
                            run = false;
                        }
                    }else {
                        Object ret = p.next();
                        outqueue.put(ret);
                    }
                    if(tpipeline.running() != true){
                        //first check to see if I can pull anything more from the inqueue
                        if(!inqueue.isEmpty()){
                            o = inqueue.take();
                            p.setStarts(Arrays.asList(o));
                            //if not, shutdown
                        }else {
                            run = false;
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void shutdown(){
        run = false;
    }

}
