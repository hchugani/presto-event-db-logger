package com.presto_myproject.listener;

import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import java.util.Map;

public class EventDBLoggerListenerFactory implements EventListenerFactory {
    @Override
    public String getName() {
        return "oracle-event-logger";
    }

    @Override
    public EventListener create(Map<String, String> config) {
        return new EventDBLoggerListener(config);
    }
}
