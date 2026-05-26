FROM python:3.11-slim

WORKDIR /app

# Copy the requirements file and install dependencies
# COPY requirements.txt .

RUN pip install --no-cache-dir websockets requests pandas numpy websocket-client finnhub-python yfinance        

# Copy the actual script
COPY n8nio_n8n_latest/main6.py .

# Run the script permanently
CMD ["python", "main6.py"]
