FROM python:3.11-slim

WORKDIR /app

# Dependencias del sistema para lxml / bs4
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Crear carpeta de datos y contactos si no existen
RUN mkdir -p data contacts

CMD ["python", "main.py"]
