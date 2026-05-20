import os
import time
from datetime import datetime, timezone

_cooldown: dict = {}
COOLDOWN_SECONDS = 1800  # 30 min per script


def _send(message: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print(f"[notify] SKIPPED — TELEGRAM_BOT_TOKEN={'set' if token else 'MISSING'} "
              f"TELEGRAM_CHAT_ID={'set' if chat_id else 'MISSING'} in container env")
        return
    try:
        import requests
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message},
            timeout=10,
        )
        if not r.ok:
            print(f"[notify] HTTP {r.status_code}: {r.text[:300]}")
    except Exception as e:
        print(f"[notify] send failed: {e}")


def notify_failure(script_name: str, reason: str, phase: str = "") -> None:
    now = time.time()
    if now - _cooldown.get(script_name, 0) < COOLDOWN_SECONDS:
        return
    _cooldown[script_name] = now
    ts = datetime.now(timezone.utc).strftime("%b %d, %H:%M UTC")
    prefix = f"{phase} FAILED — " if phase else "FAILED — "
    _send(
        f"❌ {prefix}{script_name}\n"
        f"{reason} | {ts}\n"
        f"Pipeline is still running (non-fatal)"
    )


def notify_brief(
    ticker: str,
    title: str,
    side: str,
    odds: float,
    confidence: float,
    edge: float,
) -> None:
    odds_cents = int(round(odds * 100))
    conf_pct = int(round(confidence * 100))
    edge_pct = int(round(edge * 100))
    _send(
        f"\U0001f9e0 New Brief\n"
        f"{title}\n"
        f"Buy {side} @ {odds_cents}¢ | {conf_pct}% conf | +{edge_pct}% edge"
    )


def notify_crash(traceback_text: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%b %d, %H:%M UTC")
    truncated = traceback_text[-800:] if len(traceback_text) > 800 else traceback_text
    _send(
        f"\U0001f480 ETL ORCHESTRATOR CRASHED\n"
        f"{ts} — Cycle aborted, resuming in 5 min\n\n"
        f"{truncated}"
    )
