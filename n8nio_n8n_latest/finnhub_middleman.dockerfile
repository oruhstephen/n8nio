FROM python:3.11-slim

WORKDIR /app

# Copy the requirements file and install dependencies
# COPY requirements.txt .

RUN pip install --no-cache-dir websockets requests pandas numpy websocket-client finnhub-python         

# Copy the actual script
COPY n8nio_n8n_latest/main.py .

# Run the script permanently
CMD ["python", "main.py"]
