package com.example.execution;

import com.example.ProcessingEvent;

import java.util.Random;

/**
 * stub executor of event
 */
public class EventExecutorDefault implements EventExecutor {
    private static final int MAX_EVENT_EXECUTION_TIME_MILLISECONDS = 5000;
    private final ProcessingEvent event;

    public EventExecutorDefault(ProcessingEvent event) {
        this.event = event;
    }

    @Override
    public ProcessingEvent call() throws Exception {
        System.out.println(String.format("%s started", event.toString()));
        Thread.sleep(new Random().nextInt(MAX_EVENT_EXECUTION_TIME_MILLISECONDS));
        System.out.println(String.format("%s done", event.toString()));
        return event;
    }
}
