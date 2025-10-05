# Use official Python 3.12 slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Expose port 8000 for the FastAPI server
EXPOSE 8000

# Environment variables for optional TLS
ENV SSL_CERTFILE=server.crt
ENV SSL_KEYFILE=server.key

# Run the server
CMD ["python", "server.py"]
