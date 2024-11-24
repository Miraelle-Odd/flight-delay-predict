#!/bin/bash

# Function to check if Cassandra is available
wait_for_cassandra() {
  echo "Waiting for Cassandra to be ready..."
  for i in {1..30}; do
    python <<EOF
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time

def test_connection(i):
    cassandra_hosts = ['${CASSANDRA_HOST}']  # Use the Docker service name 'cassandra'
    cassandra_port = ${CASSANDRA_PORT}
    username = 'cassandra'
    password = 'cassandra'

    try:
        auth_provider = PlainTextAuthProvider(username, password)
        cluster = Cluster(cassandra_hosts, port=cassandra_port, auth_provider=auth_provider)
        session = cluster.connect()
        #session.set_keyspace('testframe')  # or your desired keyspace
        print("Cassandra is up!")
        exit(0)
    except Exception as e:
        print(e)
        print(f"Cassandra is not up yet, retrying... ({i})")
        time.sleep(i)  # Sleep before retrying

if __name__ == "__main__":
    test_connection($i)
EOF
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
