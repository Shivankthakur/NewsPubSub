# Use the official Python image as the base
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the client source code into the container
COPY . .

# Expose the port for the client (optional, depending on your application needs)
EXPOSE 8080

# Command to run the client application
CMD ["python", "client_interface.py"]
