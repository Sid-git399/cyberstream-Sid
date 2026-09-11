FROM python:3.11-slim
WORKDIR /app
COPY generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared /app/shared
COPY generator /app/generator
WORKDIR /app/generator
ENTRYPOINT ["python", "generator.py"]
CMD ["--preset", "small", "--scenario", "normal", "--rate", "200", "--sink", "kafka"]
