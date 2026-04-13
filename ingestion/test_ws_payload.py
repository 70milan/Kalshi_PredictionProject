import websocket
import json
import base64
import time
from dotenv import load_dotenv
import os
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization

load_dotenv()
API_KEY = os.getenv("KALSHI_API_KEY")
API_SECRET = os.getenv("KALSHI_API_SECRET").replace("\\n", "\n")

def build_ws_headers() -> dict:
    timestamp = str(int(time.time() * 1000))
    msg_str = timestamp + "GET" + "/trade-api/ws/v2"
    private_key = serialization.load_pem_private_key(API_SECRET.encode(), password=None)
    signature = private_key.sign(
        msg_str.encode(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256()
    )
    return {
        "KALSHI-ACCESS-KEY": API_KEY,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
        "KALSHI-ACCESS-TIMESTAMP": timestamp
    }

def on_message(ws, message):
    data = json.loads(message)
    if data.get("type") == "ticker":
        print(json.dumps(data, indent=2))
        ws.close()

def on_open(ws):
    msg = {
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["ticker"],
            "market_tickers": ["KXFEDCHAIRCOUNT-27-B58", "KXTRUMP-24B", "KXPOPEVISIT-27JAN01-SSUD"]
        }
    }
    ws.send(json.dumps(msg))

ws = websocket.WebSocketApp(
    "wss://api.elections.kalshi.com/trade-api/ws/v2",
    header=build_ws_headers(),
    on_open=on_open,
    on_message=on_message
)

ws.run_forever(ping_interval=30, ping_timeout=10)
