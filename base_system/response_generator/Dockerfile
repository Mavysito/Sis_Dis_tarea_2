FROM python:3.11-slim

WORKDIR /app

# 1. Copiamos e instalamos dependencias primero (esto ahorra tiempo al reconstruir)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. ¡IMPORTANTE! Copiamos todo el contenido de la carpeta local al contenedor
# Esto incluye tu main.py y cualquier otro script que tengas ahí
COPY . .

# 3. No olvides que este módulo necesita leer el CSV. 
# Si el CSV está en una carpeta superior, lo mapearemos por el docker-compose,
# pero el código espera que exista la carpeta /app/data
RUN mkdir -p data

# Ejecución
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]