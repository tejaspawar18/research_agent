FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY services/extraction/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY services/extraction/ /app/

# Run the service
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8003"]
