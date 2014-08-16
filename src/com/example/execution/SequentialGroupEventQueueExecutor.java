package com.example.execution;

import com.example.ProcessingEvent;

import java.util.concurrent.*;

/**
 * <p>executes events from queue in multiple threads</p>
 * <p>it is guaranteed that events of any given group will be executed sequentially</p>
 * <p>executor also tries to keep close to overall order of events</p>
  */
public class SequentialGroupEventQueueExecutor implements EventQueueExecutor {
    private final int POLL_PERIOD_MILLISECONDS = 100;

    private final UniqueGroupProcessingEventQueue queue;
    private volatile boolean running = false;
    private Thread completionWatcher;
    private ExecutorService threadPool;

    // manually track active threads to keep event order close to original
    // otherwise from group sequence {0,0,1,2,3,4} and single thread we get
    // {0,1,2,3,4,0}
    private volatile int freeThreadCount = 0;
    private int threadCount;

    public SequentialGroupEventQueueExecutor(BlockingQueue<ProcessingEvent> queue, int threadCount) {
        this.queue = new UniqueGroupProcessingEventQueue(queue);
        this.threadCount = threadCount;
    }

    /**
     * start watching the queue
     * it's safe to call start several times
     */
    @Override
    public synchronized void start() {
        if (!isShutdown()) {
            shutdown();
            while (!isShutdown()) {
                // waiting for shutdown to complete
            }
        }
        running = true;
        queue.startPolling();
        threadPool = Executors.newFixedThreadPool(threadCount);
        freeThreadCount = threadCount;
        CompletionService<ProcessingEvent> completionService = new ExecutorCompletionService<>(threadPool);
        startQueuePollLoop(completionService);
        startCompletionWatchLoop(completionService);
    }

    /**
     * starts loop polls event and blocks further event of event's group
     * @param completionService
     */
    private void startQueuePollLoop(CompletionService<ProcessingEvent> completionService) {
        new Thread(() -> {
            while (running || !queue.isEmpty()) {
                try {
                    if (freeThreadCount == 0) {
                        Thread.sleep(POLL_PERIOD_MILLISECONDS);
                        continue;
                    }
                    ProcessingEvent event = queue.poll(POLL_PERIOD_MILLISECONDS, TimeUnit.MILLISECONDS);
                    if (event == null) continue;
                    queue.blockGroup(event.getGroupId());
                    EventExecutor executor = EventExecutorFactory.make(event);
                    completionService.submit(executor);
                    freeThreadCount--;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            threadPool.shutdown();
        }).start();
    }

    /**
     * <p>starts loop that handle completed events</p>
     * @param completionService
     */
    private void startCompletionWatchLoop(CompletionService<ProcessingEvent> completionService) {
        completionWatcher = new Thread(() -> {
            while (true) {
                try {
                    Future<ProcessingEvent> future = completionService.poll(POLL_PERIOD_MILLISECONDS, TimeUnit.MILLISECONDS);
                    if (future == null) {
                        if (threadPool.isTerminated()) break;
                        continue;
                    }
                    onEventCompleted(future.get());
                    freeThreadCount++;
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        });
        completionWatcher.start();
    }

    private void onEventCompleted(ProcessingEvent event) {
        queue.unblockGroup(event.getGroupId());
    }

    /**
     * <p>stop pulling and wait for all polled events to complete</p>
     * <p>because of event reordering this can take arbitrary long time to complete</p>
     * <p>use <code>EventProcessor#isShutdown</code> to get know when processing of already started events finished</p>
     * @see SequentialGroupEventQueueExecutor#isShutdown()
     */
    @Override
    public synchronized void shutdown() {
        running = false;
        queue.stopPolling();
    }

    /**
     * returns true if processor does not polling queue and has no active events in process
     * @return
     */
    @Override
    public boolean isShutdown() {
        // completionWatcher will die when threadPool is shut down and unblocked all potential groupId blocks
        return !running && (completionWatcher == null || !completionWatcher.isAlive());
    }

    /**
     * returns true if queue have no more events at the moment
     * @return
     */
    @Override
    public boolean isEmptyQueue() {
        return queue.isEmpty();
    }
}
