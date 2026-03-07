FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

COPY . .

# Create data directory for SQLite fallback (not used when DATABASE_URL is set)
RUN mkdir -p /app/data

EXPOSE 8000

# Use $PORT env var if set (Render injects it), otherwise default to 8000
CMD ["sh", "-c", "python -c \"import src.core.config as c; p=c.config.server_port\" 2>/dev/null; uvicorn src.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
