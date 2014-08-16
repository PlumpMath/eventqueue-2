package com.example.execution;

import com.example.ProcessingEvent;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * <p>wrapper of BlockingQueue<ProcessingEvent> that allows to postpone events for some groups
 * (i.e. <code>UniqueGroupProcessingEventQueue#poll</code> will only return events from non-blocked groups)</p>
 * <p>it is only guaranteed that order of events within any given group is stable</p>
 */
public class UniqueGroupProcessingEventQueue {
    private final BlockingQueue<ProcessingEvent> queue;
    private final Queue<ProcessingEvent> firstPriority = new ConcurrentLinkedQueue<>();
    private Map<Long, Queue<ProcessingEvent>> blockedEvents = new HashMap<>();
    private volatile boolean poolingFromSource = true;

    public UniqueGroupProcessingEventQueue(BlockingQueue<ProcessingEvent> queue) {
        this.queue = queue;
    }

    /**
     * postpone events of group
     * @param groupId
     */
    public void blockGroup(Long groupId) {
        synchronized (blockedEvents) {
            if (blockedEvents.containsKey(groupId)) return;
            blockedEvents.put(groupId, new ArrayDeque<>());
        }
    }

    /**
     * resume polling events of group
     * @param groupId
     */
    public void unblockGroup(Long groupId) {
        synchronized (blockedEvents) {
            if (!blockedEvents.containsKey(groupId)) return;
            firstPriority.addAll(blockedEvents.get(groupId));
            blockedEvents.remove(groupId);
        }
    }

    /**
     * @see java.util.concurrent.BlockingQueue#poll(long, java.util.concurrent.TimeUnit)
     */
    public ProcessingEvent poll(int timeout, TimeUnit unit) throws InterruptedException {
        long periodEnd = unit.toNanos(timeout) + System.nanoTime();

        ProcessingEvent event = null;
        while (event == null && !firstPriority.isEmpty()) {
            event = firstPriority.poll();
            synchronized (blockedEvents) {
                if (blockedEvents.containsKey(event.getGroupId())) {
                    blockedEvents.get(event.getGroupId()).add(event);
                    event = null;
                    continue;
                }
            }
        }

        while (event == null && poolingFromSource) {
            long timeoutLeft = periodEnd - System.nanoTime();
            if (timeoutLeft <= 0) return null;
            event = queue.poll(timeoutLeft, TimeUnit.NANOSECONDS);
            if (event == null) return null;
            synchronized (blockedEvents) {
                if (blockedEvents.containsKey(event.getGroupId())) {
                    blockedEvents.get(event.getGroupId()).add(event);
                    event = null;
                }
            }
        }
        return event;
    }


    /**
     * returns true if there is no blocked groups, all blocked events are polled
     * and source queue is empty or not polled anymore
     * @see java.util.concurrent.BlockingQueue#isEmpty()
     */
    public boolean isEmpty() {
        return blockedEvents.isEmpty() && firstPriority.isEmpty() && (queue.isEmpty() || !poolingFromSource);
    }

    /**
     * stop polling from source queue
     * @see UniqueGroupProcessingEventQueue#isEmpty()
     */
    public  void stopPolling() {
        poolingFromSource = false;
    }

    public void startPolling() {
        poolingFromSource = true;
    }
}
