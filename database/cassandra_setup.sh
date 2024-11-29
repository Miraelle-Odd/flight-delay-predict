#!/bin/bash

# Function to check if Cassandra is available
wait_for_cassandra() {
  echo "Waiting for Cassandra to be ready..."
  for i in {1..30}; do
    python /app/database/cassandra_connect.py
    if [ $? -eq 0 ]; then
      break
    fi
    echo "Retrying in $i seconds..."
    sleep $i
  done
}

wait_for_cassandra

# After Cassandra is ready, proceed with executing CQL commands
echo "Executing CQL files..."

# Iterate over the CQL files in /app/table
for file in /app/database/*.cql; do
  [ -e "$file" ] || continue
  echo "Executing $file..."
  docker exec -i cassandra cqlsh -f "$file" || { echo "Error executing $file"; exit 1; }
done

echo "Done."
exit 0
