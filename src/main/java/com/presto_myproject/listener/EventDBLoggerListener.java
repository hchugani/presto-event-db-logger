package com.presto_myproject.listener;

import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import java.util.List;
import java.util.Map;
import java.sql.*;
import oracle.jdbc.pool.*;
import java.util.logging.Logger;

public class EventDBLoggerListener implements EventListener {

    private Map<String,String> config;
    private OracleDataSource ods;
    private Logger log;
    private final String loggerName = getClass().getName();

    public EventDBLoggerListener(Map<String,String> configParams) {
        log = Logger.getLogger(loggerName);
        config = configParams;
        createOracleConnection();
    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        try{
            log.info("Query Completed: " + queryCompletedEvent.getMetadata().getQuery());
            Connection conn = ods.getConnection();
            Statement statement = conn.createStatement();

            //Get Query Metadata
            String queryId = queryCompletedEvent.getMetadata().getQueryId();
            String query = queryCompletedEvent.getMetadata().getQuery();
            String queryState = queryCompletedEvent.getMetadata().getQueryState();
            String catalog = queryCompletedEvent.getContext().getCatalog().get();
            String schema = queryCompletedEvent.getContext().getSchema().get();
            String user = queryCompletedEvent.getContext().getUser();
            String userPrincipal = queryCompletedEvent.getContext().getPrincipal().get();
            String sessionProperties = queryCompletedEvent.getContext().getSessionProperties().toString();
            queryCompletedEvent.getContext().getEnvironment();
            queryCompletedEvent.getContext().getSource().get();
            queryCompletedEvent.getContext().getServerAddress();
            queryCompletedEvent.getContext().getServerVersion();
        }
        catch (Exception ex){
            log.info(ex.getMessage());
        }
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
