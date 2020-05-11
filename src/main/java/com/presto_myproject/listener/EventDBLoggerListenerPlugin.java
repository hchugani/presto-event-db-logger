package com.presto_myproject.listener;

import io.prestosql.spi.Plugin;
import io.prestosql.spi.eventlistener.EventListenerFactory;

import java.util.ArrayList;
import java.util.List;

public class EventDBLoggerListenerPlugin implements Plugin{
    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories() {
        EventListenerFactory dbLoggerListenerFactory = new EventDBLoggerListenerFactory();
        List<EventListenerFactory> factoryList = new ArrayList<EventListenerFactory>();
        factoryList.add(dbLoggerListenerFactory);
        return factoryList;
    }
}
