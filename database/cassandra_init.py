from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def test_connection():
    # Update these parameters as needed
    cassandra_hosts = ['127.0.0.1']  # Replace with your Cassandra node IP(s)
    cassandra_port = 9042           # Default Cassandra port
    keyspace_name = 'test_keyspace' # Replace with your keyspace

    # Authentication (if enabled in Cassandra)
    username = 'cassandra'          # Default username
    password = 'cassandra'          # Default password

    try:
        # Connect to the Cassandra cluster
        auth_provider = PlainTextAuthProvider(username, password)
        cluster = Cluster(cassandra_hosts, port=cassandra_port, auth_provider=auth_provider)
        session = cluster.connect()

        # Print cluster metadata
        print("Connected to cluster:", cluster.metadata.cluster_name)

        # Check if the keyspace exists; if not, create it
        session.set_keyspace(keyspace_name)
        print(f"Using keyspace: {keyspace_name}")

        # Run a simple query to verify the connection
        # session.execute("CREATE TABLE IF NOT EXISTS test_table (id UUID PRIMARY KEY, name TEXT)")
        # session.execute("INSERT INTO test_table (id, name) VALUES (uuid(), 'test_name')")
        rows = session.execute("SELECT * FROM test_table")

        print("Data from test_table:")
        for row in rows:
            print(row)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Clean up
        if 'cluster' in locals():
            cluster.shutdown()

if _name_ == '_main_':
    test_connection()