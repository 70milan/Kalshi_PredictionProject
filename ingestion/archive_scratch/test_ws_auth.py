"""
Minimal Kalshi WebSocket auth test — isolates the 401 root cause.
Tests both domains and prints debug info.
"""
import os, time, base64, json
import websocket
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

load_dotenv()

API_KEY    = os.getenv("KALSHI_API_KEY")
API_SECRET = os.getenv("KALSHI_API_SECRET").replace("\\n", "\n")

print(f"API_KEY loaded: {API_KEY[:8]}...{API_KEY[-4:]}" if API_KEY else "API_KEY: MISSING!")
print(f"API_SECRET loaded: {'YES' if API_SECRET and 'BEGIN' in API_SECRET else 'NO / MALFORMED'}")
print(f"SECRET starts with: {API_SECRET[:30]}..." if API_SECRET else "")
print()

# ─── Build auth headers ───
def build_headers(ws_path: str) -> list:
    timestamp   = str(int(time.time() * 1000))
    message     = timestamp + "GET" + ws_path
    
    print(f"  Signing message: '{message[:40]}...'")
    print(f"  Timestamp (ms):  {timestamp}")
    
    private_key = serialization.load_pem_private_key(API_SECRET.encode(), password=None)
    
    # Try RSA-PSS first (documented requirement)
    signature = private_key.sign(
        message.encode(),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    sig_b64 = base64.b64encode(signature).decode()
    
    print(f"  Signature (PSS): {sig_b64[:30]}...")
    
    return [
        f"KALSHI-ACCESS-KEY: {API_KEY}",
        f"KALSHI-ACCESS-SIGNATURE: {sig_b64}",
        f"KALSHI-ACCESS-TIMESTAMP: {timestamp}",
    ]


# ─── Test connection ───
def test_connect(label: str, url: str, path: str):
    print(f"\n{'='*60}")
    print(f"TEST: {label}")
    print(f"URL:  {url}")
    print(f"Path: {path}")
    print(f"{'='*60}")
    
    headers = build_headers(path)
    
    result = {"connected": False, "error": None}
    
    def on_open(ws):
        result["connected"] = True
        print(f"  >>> CONNECTED! Auth succeeded.")
        ws.close()
    
    def on_error(ws, error):
        result["error"] = str(error)
        print(f"  >>> ERROR: {error}")
    
    def on_close(ws, code, msg):
        print(f"  >>> CLOSED. Code={code} Msg={msg}")
    
    ws = websocket.WebSocketApp(
        url,
        header=headers,
        on_open=on_open,
        on_error=on_error,
        on_close=on_close,
    )
    
    # Run with a 5-second timeout
    import threading
    timer = threading.Timer(5.0, ws.close)
    timer.daemon = True
    timer.start()
    
    ws.run_forever(ping_interval=0)
    timer.cancel()
    
    return result


# ─── Run tests ───
if __name__ == "__main__":
    # Test 1: Elections domain (what you've been using)
    r1 = test_connect(
        "Elections Domain (PSS)",
        "wss://api.elections.kalshi.com/trade-api/ws/v2",
        "/trade-api/ws/v2"
    )
    
    time.sleep(1)
    
    # Test 2: Main trading domain (documented in examples)
    r2 = test_connect(
        "Main Trading Domain (PSS)", 
        "wss://trading-api.kalshi.com/trade-api/ws/v2",
        "/trade-api/ws/v2"
    )
    
    time.sleep(1)
    
    # Test 3: Elections domain with PKCS1v15 (just in case)
    print(f"\n{'='*60}")
    print("TEST: Elections Domain (PKCS1v15 fallback)")
    print(f"{'='*60}")
    
    timestamp = str(int(time.time() * 1000))
    path = "/trade-api/ws/v2"
    message = timestamp + "GET" + path
    private_key = serialization.load_pem_private_key(API_SECRET.encode(), password=None)
    signature = private_key.sign(message.encode(), padding.PKCS1v15(), hashes.SHA256())
    sig_b64 = base64.b64encode(signature).decode()
    
    headers_pkcs = [
        f"KALSHI-ACCESS-KEY: {API_KEY}",
        f"KALSHI-ACCESS-SIGNATURE: {sig_b64}",
        f"KALSHI-ACCESS-TIMESTAMP: {timestamp}",
    ]
    
    r3 = {"connected": False, "error": None}
    
    def on_open3(ws):
        r3["connected"] = True
        print(f"  >>> CONNECTED! PKCS1v15 auth succeeded.")
        ws.close()
    
    def on_error3(ws, error):
        r3["error"] = str(error)
        print(f"  >>> ERROR: {error}")
    
    def on_close3(ws, code, msg):
        print(f"  >>> CLOSED. Code={code} Msg={msg}")
    
    ws3 = websocket.WebSocketApp(
        "wss://api.elections.kalshi.com/trade-api/ws/v2",
        header=headers_pkcs,
        on_open=on_open3,
        on_error=on_error3,
        on_close=on_close3,
    )
    timer3 = threading.Timer(5.0, ws3.close)
    timer3.daemon = True
    timer3.start()
    ws3.run_forever(ping_interval=0)
    timer3.cancel()
    
    # Summary
    print(f"\n{'='*60}")
    print("RESULTS SUMMARY")
    print(f"{'='*60}")
    print(f"  Elections + PSS:     {'PASS' if r1['connected'] else 'FAIL'}")
    print(f"  Trading + PSS:      {'PASS' if r2['connected'] else 'FAIL'}")
    print(f"  Elections + PKCS15: {'PASS' if r3['connected'] else 'FAIL'}")
