# presto-event-db-logger

This plugin can be used for audit logging presto queries in Database.

# Usage
1) The JAR file should be added to the plugin directory .
2) Create etc/event-listener.properties file with the entries as below

event-listener.name=oracle-event-logger
connection-url=thin
connection-user=ORACLE_USER
connection-password=PASSWORD
logging-table=<SCHEMA_NAME>.<LOGGING_TABLE>

