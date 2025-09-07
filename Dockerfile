# Use the official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Set work directory
WORKDIR /app

# Install system dependencies
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      git \
      ca-certificates \
      gcc \
      g++ \
 && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Raven Utils with authentication
# Note: Pass token as build arg: docker build --build-arg GITHUB_TOKEN=your_token .
ARG GITHUB_USER
ARG GITHUB_TOKEN
RUN pip install --no-cache-dir \
  "git+https://${GITHUB_USER}:${GITHUB_TOKEN}@github.com/AdminRavenData/Raven_Utils.git@main"

# Copy project files
COPY . .

# Expose the port the app runs on
EXPOSE 8080

# Entry point - run the Flask application
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 0 main:app