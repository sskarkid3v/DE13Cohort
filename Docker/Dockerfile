# Dockerfile
FROM python:3.12-slim

# Prevent Python from writing .pyc files and ensure logs flush immediately
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app ./app

# Default command (Compose can override)
CMD ["python", "-m", "app.main"]
