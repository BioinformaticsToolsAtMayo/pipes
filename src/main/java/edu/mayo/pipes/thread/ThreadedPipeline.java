package edu.mayo.pipes.thread;

import com.tinkerpop.pipes.transform.IdentityPipe;
import edu.mayo.pipes.PrintPipe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: m102417
 * Date: 9/2/13
 * Time: 9:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class ThreadedPipeline<S,E> {

    public ThreadedPipeline() throws InterruptedException {
        init();
    }

    private boolean run = true;
    List<BlockingQueue> queues;
    protected Producer<S,?> producer;
    protected Consumer<?,E> consumer;
    protected List filters;

    /**
     * pipeline works like this:
     * producer -queue1-> Filter1 -queue2-> Filter2 -queue3-> ... -queueN-> consumer
     * Each filter, producer and consumer is its own thread, passing data down the pipeline for the next transformation
     * @throws InterruptedException
     */
    public void init() throws InterruptedException {
        queues = new ArrayList<BlockingQueue>();
        BlockingQueue queue = new ArrayBlockingQueue(1024);
        queues.add(queue);

        producer = new Producer(queues.get(0), new IdentityPipe(), this);
        consumer = new Consumer(queues.get(0), new PrintPipe(), this);

        new Thread(producer).start();
        new Thread(consumer).start();

        //Thread.sleep(2000);

        //consumer.shutdown();
    }



    public static void main(String[] args) throws Exception {
           ThreadedPipeline tpipeline = new ThreadedPipeline();

    }

    public synchronized boolean running(){
        return run;
    }

    public synchronized void shutdown(){
        run = false;
    }

}
