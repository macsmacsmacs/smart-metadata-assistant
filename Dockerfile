FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY metadata_lookup.py metadata_mcp_server.py ./

EXPOSE 8080
ENV PORT=8080

# Cloud Run sets PORT; default to 8080 for local runs
CMD ["sh", "-c", "exec uvicorn metadata_mcp_server:app --host 0.0.0.0 --port ${PORT:-8080}"]
