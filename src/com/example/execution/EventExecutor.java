package com.example.execution;

import com.example.ProcessingEvent;

import java.util.concurrent.Callable;

/**
 * executor performs operations according to event and returns it
 */
public interface EventExecutor extends Callable<ProcessingEvent> {
}
