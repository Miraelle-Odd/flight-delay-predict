import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def test_connection():
    cassandra_hosts = [os.getenv('CASSANDRA_HOST', 'localhost')]  # Default to 'localhost' if not set
    cassandra_port = int(os.getenv('CASSANDRA_PORT', 9042))    # Default to 9042 if not set
    username = os.getenv('CASSANDRA_USERNAME', 'cassandra')    # Default to 'cassandra'
    password = os.getenv('CASSANDRA_PASSWORD', 'cassandra')    # Default to 'cassandra'

    try:
        auth_provider = PlainTextAuthProvider(username, password)
        cluster = Cluster(cassandra_hosts, port=cassandra_port, auth_provider=auth_provider)
        session = cluster.connect()
        #session.set_keyspace('testframe')  # or your desired keyspace
        print("Cassandra is up!")
        exit(0)
    except Exception as e:
        print(cassandra_hosts)
        print(cassandra_port)
        print(e)
        print(f"Cassandra is not up yet, retrying... ")

if __name__ == "__main__":
    test_connection()