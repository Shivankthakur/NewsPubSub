# Use the official Python image as the base
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the broker source code into the container
COPY . .

# Expose the port (we'll use environment variables to set it dynamically)
EXPOSE 3000-3100

# Run the broker application
CMD ["python", "broker.py", "--broker_id", "${BROKER_ID}", "--port", "${PORT}", "--peers", "${PEERS}"]
