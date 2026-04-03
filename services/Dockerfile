FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shopcommon ./shopcommon
ARG SERVICE
COPY ${SERVICE} ./${SERVICE}
ENV SERVICE=${SERVICE}

RUN mkdir -p /data
EXPOSE 8000
CMD ["sh", "-c", "uvicorn ${SERVICE}.app:app --host 0.0.0.0 --port 8000"]
