FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy shared code
COPY shared/ /app/shared/

# Copy requirements and install dependencies
COPY services/crawler/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY services/crawler/__init__.py /app/
COPY services/crawler/*.py /app/

EXPOSE 8001

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]