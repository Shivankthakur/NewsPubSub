# Dockerfile
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy requirements.txt to the container
COPY broker/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all files from the broker directory to the /app directory inside the container
COPY broker/ /app/

# Set environment variables (BROKER_ID and PEERS should be set in docker-compose)
ENV BROKER_ID=1
ENV PEERS="2,3,4,5"

# Expose the service port
EXPOSE 8080

# Run the application
CMD ["python", "broker.py"]
