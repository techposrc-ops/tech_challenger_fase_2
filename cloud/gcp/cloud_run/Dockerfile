FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

COPY src ./src

ENV PYTHONPATH=/app/src

RUN pip install --no-cache-dir \
    "confluent-kafka>=2.6,<3" \
    "flask>=3.0,<4" \
    "google-auth>=2.35,<3" \
    "gunicorn>=23,<24" \
    "requests>=2.32,<3" \
    "urllib3>=2.2,<3"

EXPOSE 8080

CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:${PORT} --workers 1 --threads 4 --timeout 120 alfabetizacao.streaming.producer:app"]
