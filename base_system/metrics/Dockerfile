# Usamos una imagen ligera de Python
FROM python:3.11-slim

# Establecemos el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiamos primero el requirements para aprovechar la caché de Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto del código del módulo
COPY . .

# Comando para ejecutar el script
# Nota: Para 'response_generator' y 'metrics' usa uvicorn. 
# Para 'traffic_generator' usa python main.py
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]