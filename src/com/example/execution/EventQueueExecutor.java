package com.example.execution;

public interface EventQueueExecutor {
    void start();
    void shutdown();
    boolean isShutdown();
    boolean isEmptyQueue();
}
