FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api.py .
COPY pipeline.py .
COPY ai/ ai/
COPY serper/ serper/

ENV PYTHONUNBUFFERED=1
ENV PORT=8080
EXPOSE 8080

CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port ${PORT:-8080} --timeout-keep-alive 300"]
