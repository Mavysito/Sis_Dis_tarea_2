# 1. Imagen base liviana
FROM python:3.11-slim

# 2. Directorio de trabajo
WORKDIR /app

# 3. Instalamos dependencias (requests y numpy)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiamos el script
COPY main.py .

# 5. EJECUCIÓN (Aquí está la diferencia clave)
# No usamos uvicorn porque no es una API. Usamos python directo.
# El flag -u es para que veas los prints en tiempo real en tu terminal de Linux
CMD ["python", "-u", "main.py"]