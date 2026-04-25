FROM python:3.11-slim

ARG SERVICE=aggregator

WORKDIR /app

RUN adduser --disabled-password --gecos '' appuser \
    && mkdir -p /app/data \
    && chown -R appuser:appuser /app

USER appuser

COPY --chown=appuser:appuser requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=appuser:appuser src/${SERVICE}/ ./src/

CMD ["python", "-m", "src.main"]
