package com.presto_myproject.listener;

import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import java.util.List;
import java.util.Map;
import java.sql.*;
import oracle.jdbc.pool.*;
import java.util.logging.Logger;

public class DBEventLoggerListener implements EventListener {

    private Map<String,String> config;
    private OracleDataSource ods;
    private Logger log;
    private final String loggerName = getClass().getName();

    public DBEventLoggerListener(Map<String,String> configParams) {
        log = Logger.getLogger(loggerName);
        config = configParams;
        createOracleConnection();
    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {

    }

    private void createOracleConnection(){
        try{
            ods = new OracleDataSource();
            ods.setURL(config.get("connection-url"));
            ods.setUser(config.get("connection-user"));
            ods.setPassword(config.get("connection-password"));
        }
        catch (SQLException ex){
            log.info(ex.getMessage());
        }
    }
}
