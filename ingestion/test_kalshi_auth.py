import os
import requests
import base64
import time
from datetime import datetime
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

# Load .env
load_dotenv()

# 1. Setup Your Credentials
api_key = os.getenv("KALSHI_API_KEY")
api_secret = os.getenv("KALSHI_API_SECRET")

if not api_key or not api_secret:
    print("❌ ERROR: Keys missing in .env")
    exit()

# Clean secret
api_secret_bytes = api_secret.replace("\\n", "\n").encode()

# 2. Preparation for Signing
method = "GET"
path = "/trade-api/v2/markets"
timestamp = str(int(time.time() * 1000))

# Message format
msg = timestamp + method + path
print(f"Signing message: {msg}")

try:
    # 3. Load RSA Private Key
    private_key = serialization.load_pem_private_key(
        api_secret_bytes,
        password=None
    )

    # 4. Generate Signature
    signature = private_key.sign(
        msg.encode(),
        padding.PKCS1v15(),
        hashes.SHA256()
    )
    base64_sig = base64.b64encode(signature).decode()

    # 5. Set Headers
    headers = {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-SIGNATURE": base64_sig,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "Content-Type": "application/json"
    }

    # 6. Make Request
    url = f"https://api.elections.kalshi.com{path}"
    print("Connecting to Kalshi...")

    response = requests.get(
        url,
        headers=headers,
        params={"limit": 20, "status": "open"}
    )

    if response.status_code == 200:
        print("✅ SUCCESS! Your keys and signing logic are perfect.")
        print("Markets found:", [m['title'] for m in response.json()['markets']])
    else:
        print(f"❌ FAILED: {response.status_code}")
        print(f"Reason: {response.text}")

except Exception as e:
    print(f"❌ ERROR: {e}")