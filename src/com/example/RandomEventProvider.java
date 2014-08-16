package com.example;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * generate stream of events of random groups to queue
 * itemId of events are ascending integers
 */
public class RandomEventProvider {
    private final Queue<ProcessingEvent> queue;
    private final int maxGroupId;
    private int maxPause;
    private volatile boolean running = false;
    private Random random = new Random();
    private int lastItemId = 0;

    /**
     * @param queue target queue
     * @param maxGroupId groups will have id [0..maxGroupId)
     * @param maxPause max period between events
     */
    public RandomEventProvider(Queue<ProcessingEvent> queue, int maxGroupId, int maxPause) {
        this.queue = queue;
        this.maxGroupId = maxGroupId;
        this.maxPause = maxPause;
    }

    /**
     * starts new thread that will enqueue new events
     */
    public void start() {
        running = true;
        Thread thread = new Thread(() -> {
            while (running) {
                ProcessingEvent event = generateEvent();
                System.out.printf("%s queued%n", event.toString());
                queue.offer(event); // add or drop on the floor. You don't want this on production
                try {
                    Thread.sleep(random.nextInt(maxPause));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
            }
        });
        thread.start();
    }

    private ProcessingEvent generateEvent() {
        ProcessingEvent event = new ProcessingEvent();
        event.setItemId(lastItemId++);
        event.setGroupId(random.nextInt(maxGroupId));
        return event;
    }

    /**
     * stop producing new events
     */
    public void shutdown() {
        running = false;
    }
}
