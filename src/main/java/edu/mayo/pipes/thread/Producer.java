package edu.mayo.pipes.thread;

import com.tinkerpop.pipes.Pipe;

import java.util.concurrent.BlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: m102417
 * Date: 9/2/13
 * Time: 9:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class Producer<S,E> implements Runnable{

    private ThreadedPipeline tpipeline;
    protected BlockingQueue outqueue = null;

    public Producer(BlockingQueue queue, Pipe p, ThreadedPipeline threadedPipeline) {
        this.outqueue = queue;
        this.tpipeline = threadedPipeline;
    }

    public void run() {
        try {
            for(int i=0;i<100000;i++){
                outqueue.put(i);
            }
//            outqueue.put("1");
//            //Thread.sleep(1000);
//            outqueue.put("2");
//            //Thread.sleep(1000);
//            outqueue.put("3");
            tpipeline.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
