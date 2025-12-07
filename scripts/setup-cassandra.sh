#!/bin/bash

echo "Waiting for Cassandra to be ready..."
max_attempts=45
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if docker exec cassandra cqlsh -e "describe cluster" > /dev/null 2>&1; then
        echo "Cassandra is ready!"
        break
    fi
    attempt=$((attempt + 1))
    echo "Attempt $attempt/$max_attempts - Cassandra not ready yet..."
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "Cassandra failed to start within expected time"
    exit 1
fi

echo "Running initialization script..."
docker exec -i cassandra cqlsh < scripts/init.cql

echo "Cassandra setup complete!"
echo ""
echo "You can verify with:"
echo "  docker exec -it cassandra cqlsh"
echo "  USE vylet;"
echo "  DESCRIBE TABLES;"
