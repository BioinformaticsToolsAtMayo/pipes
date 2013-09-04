package edu.mayo.pipes.thread;

import com.tinkerpop.pipes.Pipe;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: m102417
 * Date: 9/2/13
 * Time: 9:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class Consumer<S,E> implements Runnable{

    protected BlockingQueue inqueue = null;
    private Pipe p;
    private ThreadedPipeline tpipeline = null;
    private boolean run = true;

    /**
     *
     * @param queue  the input queue
     * @param pipe   usually the print pipe, but it could be a write pipe, a logger or anything
     */
    public Consumer(BlockingQueue queue, Pipe pipe, ThreadedPipeline threadedPipeline) {
        this.inqueue = queue;
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
                        p.next();
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