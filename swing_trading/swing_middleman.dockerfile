FROM python:3.11-slim

WORKDIR /app

# Copy the requirements file and install dependencies
# COPY requirements.txt .

RUN pip install --no-cache-dir websockets datetime requests pandas numpy websocket-client finnhub-python yfinance        

# Copy the actual script
COPY /swing_trading/swing.py .

# Run the script permanently
CMD ["python", "swing.py"]
