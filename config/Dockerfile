# Use a Python base image
FROM python:3.9-slim

# Install required dependencies
RUN apt-get update && apt-get install -y gcc libssl-dev curl

# Install the Cassandra Python driver
RUN pip install cassandra-driver

# Set the working directory
WORKDIR /app

# Copy the database folder into the container
COPY ./database /app/database

# Set the entrypoint to run the setup script
ENTRYPOINT ["bash", "/app/database/cassandra_setup.sh"]
