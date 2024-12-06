# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose necessary ports
# Registry port
EXPOSE 4000  
# Broker ports
EXPOSE 3000-3009  

# Command to start both services (Broker and Registry)
CMD ["sh", "-c", "python3 registry.py & python3 broker.py"]
