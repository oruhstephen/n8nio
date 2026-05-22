import websocket
import json
import requests
import time

FINNHUB_TOKEN = "d86fpu1r01qgiu458c80d86fpu1r01qgiu458c8g"
N8N_WEBHOOK_URL = "https://go90ng-n8n.eq7icp.easypanel.host/webhook-test/bcca44dc-8944-41a2-8d96-3c5eb1f159e9"

# We will store trades here temporarily to calculate averages
trade_buffer = []

def on_message(ws, message):
    data = json.loads(message)
    
    # Finnhub sends ping messages, we only want actual trade data ('trade' type)
    if data['type'] == 'trade':
        for trade in data['data']:
            # Example: We only care about AAPL right now
            if trade['s'] == 'AAPL':
                trade_buffer.append(trade['p']) # Append the price
                
                # AGGREGATION LOGIC:
                # Instead of sending every single tick to n8n, wait until we have 50 trades,
                # calculate the average, and send THAT to n8n.
                if len(trade_buffer) >= 50:
                    avg_price = sum(trade_buffer) / len(trade_buffer)
                    
                    print(f"Triggering n8n! AAPL 50-tick moving average is: {avg_price}")
                    
                    # POST the clean, aggregated data to your n8n workflow
                    payload = {"symbol": "AAPL", "average_price": avg_price}
                    requests.post(N8N_WEBHOOK_URL, json=payload)
                    
                    # Clear the buffer to start calculating the next batch
                    trade_buffer.clear()

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

def on_open(ws):
    # Subscribe to the symbols
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    # ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')

if __name__ == "__main__":
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    
    # This keeps the connection open permanently
    ws.run_forever()
