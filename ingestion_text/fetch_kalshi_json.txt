import os
import time
import base64
import requests
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization
import json

# 1. Load keys
load_dotenv()
API_KEY = os.getenv("KALSHI_API_KEY")
API_SECRET = os.getenv("KALSHI_API_SECRET").replace("\\n", "\n")

# 2. Build PKCS1v15 Auth Headers (required for Kalshi REST)
path = "/trade-api/v2/series"
timestamp = str(int(time.time() * 1000))
message = timestamp + "GET" + path
private_key = serialization.load_pem_private_key(API_SECRET.encode(), password=None)
signature = private_key.sign(message.encode(), padding.PKCS1v15(), hashes.SHA256())

headers = {
    "KALSHI-ACCESS-KEY": API_KEY,
    "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
    "KALSHI-ACCESS-TIMESTAMP": timestamp,
    "Content-Type": "application/json"
}

# 3. Make Request (Example: Fetch Political Series)
print(f"Fetching {path} ...\n")
response = requests.get(f"https://api.elections.kalshi.com{path}", headers=headers)
data = response.json()

# 4. Print just the first item to see the raw JSON structure
if "series" in data and len(data["series"]) > 0:
    print(json.dumps(data["series"][0], indent=2))
else:
    print(data)
