package com.example.execution;

import com.example.ProcessingEvent;

public class EventExecutorFactory {
    public static EventExecutor make(ProcessingEvent event) {
        return new EventExecutorDefault(event);
    }
}
