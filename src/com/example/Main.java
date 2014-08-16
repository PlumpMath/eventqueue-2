package com.example;

import com.example.execution.EventQueueExecutor;
import com.example.execution.SequentialGroupEventQueueExecutor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<ProcessingEvent> queue = new ArrayBlockingQueue<>(1000);

        RandomEventProvider provider = new RandomEventProvider(queue, 6, 1000);
        provider.start();

        EventQueueExecutor consumer = new SequentialGroupEventQueueExecutor(queue, 4);
        consumer.start();

        Thread.sleep(10000L);

        provider.shutdown();

        while (!consumer.isEmptyQueue()) {
            // wait for all events to be polled for the sake of demo
            // in production you'd like to keep consumer up all the time
        }

        consumer.shutdown();

        while (!consumer.isShutdown()) {
            // wait for all events to be processed
        }
    }
}
