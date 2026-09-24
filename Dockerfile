FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && python -c "import duckdb; duckdb.sql('INSTALL httpfs; INSTALL postgres; INSTALL icu')"
ENV PYTHONPATH=/app/processor:/app/api PYTHONUNBUFFERED=1
COPY . .
