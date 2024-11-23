# Use Python as the base image
FROM python:3.10

# Set the working directory inside the container
WORKDIR /broker

# Copy the broker code and requirements
COPY . /broker

# Install required dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port that the broker will use
EXPOSE 8080

# Start the broker
CMD ["python", "broker.py"]
