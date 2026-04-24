FROM python:3.11-slim

WORKDIR /app

RUN adduser --disabled-password --gecos '' appuser \
    && chown -R appuser:appuser /app

USER appuser

COPY --chown=appuser:appuser requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=appuser:appuser src/ ./src/

EXPOSE 8081

CMD ["python", "-m", "src.main"]
