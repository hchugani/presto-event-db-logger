package com.presto_myproject.listener;

import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import java.util.Map;
import java.sql.*;

import io.prestosql.spi.eventlistener.QueryCreatedEvent;
import io.prestosql.spi.eventlistener.SplitCompletedEvent;
import oracle.jdbc.pool.*;
import java.util.logging.Logger;
import java.time.Duration;
import java.time.Instant;

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

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        try{
            log.info("Query Completed: " + queryCompletedEvent.getMetadata().getQuery());
            Connection conn = ods.getConnection();
            Statement statement = conn.createStatement();

            //Get Query Metadata
            String queryId = queryCompletedEvent.getMetadata().getQueryId();
            String query = queryCompletedEvent.getMetadata().getQuery();
            String queryState = queryCompletedEvent.getMetadata().getQueryState();
            String catalog = queryCompletedEvent.getContext().getCatalog().isPresent() ?
                    queryCompletedEvent.getContext().getCatalog().get(): "";
            String schema = queryCompletedEvent.getContext().getSchema().isPresent() ?
                    queryCompletedEvent.getContext().getSchema().get() : "";
            String user = queryCompletedEvent.getContext().getUser();
            String userPrincipal = queryCompletedEvent.getContext().getPrincipal().isPresent() ?
                    queryCompletedEvent.getContext().getPrincipal().get() : "";
            String sessionProperties = queryCompletedEvent.getContext().getSessionProperties().toString();
            String env = queryCompletedEvent.getContext().getEnvironment();
            String source = queryCompletedEvent.getContext().getSource().isPresent() ?
                    queryCompletedEvent.getContext().getSource().get() : "";
            String serverAddress = queryCompletedEvent.getContext().getServerAddress();
            String serverVersion = queryCompletedEvent.getContext().getServerVersion();
            Long totalRows = queryCompletedEvent.getStatistics().getTotalRows();
            Long totalBytes = queryCompletedEvent.getStatistics().getTotalBytes();
            Long outputRows = queryCompletedEvent.getStatistics().getOutputRows();
            Long outputBytes = queryCompletedEvent.getStatistics().getOutputBytes();
            Duration cpuTimeMs = queryCompletedEvent.getStatistics().getCpuTime();
            Duration execTime = queryCompletedEvent.getStatistics().getExecutionTime().isPresent() ?
                    queryCompletedEvent.getStatistics().getExecutionTime().get() : null;
            Instant createTime = queryCompletedEvent.getCreateTime();
            Instant endTime = queryCompletedEvent.getEndTime();

            String insertQuery = String.format("INSERT INTO " + config.get("logging-table") + " ("+
                    "query_id," +
                    "query," +
                    "query_state," +
                    "catalog," +
                    "schema," +
                    "user," +
                    "user_principal," +
                    "session_properties," +
                    "environment," +
                    "source," +
                    "server_address," +
                    "server_version," +
                    "total_rows," +
                    "total_bytes," +
                    "output_rows," +
                    "output_bytes," +
                    "cpu_time_ms," +
                    "exec_time," +
                    "create_time," +
                    "end_time" +
                    ") VALUES (" +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'," +
                    "'%s'" +
                    ")",
                    queryId,
                    query,
                    queryState,
                    catalog,
                    schema,
                    user,
                    userPrincipal,
                    sessionProperties,
                    env,
                    source,
                    serverAddress,
                    serverVersion,
                    totalRows,
                    totalBytes,
                    outputRows,
                    outputBytes,
                    cpuTimeMs,
                    execTime,
                    createTime,
                    endTime);
            log.info(insertQuery);
            statement.executeUpdate(insertQuery);
            conn.close();

        }
        catch (Exception ex){
            log.info(ex.getMessage());
            ex.printStackTrace();
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
        // logic for query creation can be added in this method
        return;
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent) {
        // logic for split completion can be added in this method
        return;
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
            ex.printStackTrace();
        }
    }
}
