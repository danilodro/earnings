# Define a imagem base
FROM python:3.9

# Copia os arquivos do diretorio atual para dentro do container
COPY . /app

# Define o diretorio de trabalho
WORKDIR /app

# Instala as dependencias
RUN pip install fastapi
RUN pip install fastapi pymongo httpx
RUN pip install uvicorn
RUN pip install python-dateutil
# Porta do container
EXPOSE 1508

# CMD para iniciar o app
CMD [ "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "1508" ]
