#!/bin/bash

# Start SQL Server in background
/opt/mssql/bin/sqlservr &

# Wait for SQL Server to be ready
echo "Waiting for SQL Server to start..."
until /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -Q "SELECT 1" &> /dev/null
do
    echo "SQL Server is starting up..."
    sleep 2
done

echo "SQL Server is ready!"

# Create database and run DDL
echo "Creating database and running init scripts..."
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -Q "CREATE DATABASE eventstore"
# -I enables QUOTED_IDENTIFIER (required for JSON INDEX)
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -I -d eventstore -i /init/tsql-event-store.ddl

echo "Database initialized successfully!"

# Keep container running
wait

