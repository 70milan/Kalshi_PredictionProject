"""
PredictIQ - Portfolio Risk Guardrails
=====================================
Fix C of the risk guardrail set (added 2026-05-18).

Problem this solves: the book held TWO Powell/Fed positions at once
(KXPOWELLPROTEMP and KXLEAVEPOWELLGOV), both sized to the per-trade cap,
both losing on the same real-world news. Per-trade Kelly sizing has no idea
the bets are correlated. This module groups briefs by theme and caps the
TOTAL dollar exposure any single theme can take.

Two pieces:
  infer_theme(ticker, title) -> theme key (groups correlated markets)
  apply_theme_caps(briefs, bankroll) -> briefs with Kelly bets clamped per theme
"""

# Aggregate exposure cap per theme, as a fraction of bankroll.
# Above the 5% single-trade hard cap so one full-size bet always fits,
# but low enough that a theme can't be doubled up to ~10% as Powell was.
THEME_CAP_FRACTION = 0.08

# Keyword -> theme. Checked in order; first hit wins. Lowercase keys.
# A theme groups markets that move together on the same news. The Powell
# entries land in "jerome-powell" because both titles contain "powell".
_THEME_KEYWORDS = [
    ("powell",            "jerome-powell"),
    ("federal reserve",   "federal-reserve"),
    ("jerome powell",     "jerome-powell"),
    ("the fed",           "federal-reserve"),
    ("interest rate",     "federal-reserve"),
    ("zelensky",          "russia-ukraine"),
    ("putin",             "russia-ukraine"),
    ("ukraine",           "russia-ukraine"),
    ("gaza",              "israel-gaza"),
    ("israel",            "israel-gaza"),
    ("netanyahu",         "israel-gaza"),
    ("trump",             "trump"),
    ("jd vance",          "trump-admin"),
    ("vance",             "trump-admin"),
    ("kash patel",        "trump-admin"),
    ("patel",             "trump-admin"),
    ("letitia james",     "trump-doj"),
    ("ingrassia",         "trump-doj"),
    ("special counsel",   "trump-doj"),
    ("doj",               "trump-doj"),
    ("biden",             "biden"),
    ("elon musk",         "tech-leaders"),
    ("musk",              "tech-leaders"),
    ("zuckerberg",        "tech-leaders"),
    ("tisch",             "nypd"),
    ("nypd",              "nypd"),
]


def infer_theme(ticker: str, title: str) -> str:
    """
    Return a theme key grouping markets that move together on shared news.
    Falls back to the Kalshi series ticker (text before the first '-') so
    every market still gets a stable group even if no keyword matches.
    """
    haystack = f"{title or ''}".lower()
    for keyword, theme in _THEME_KEYWORDS:
        if keyword in haystack:
            return theme
    # Fallback: series ticker prefix, e.g. "KXPOWELLPROTEMP-26MAY-JUN01" -> "kxpowellprotemp"
    series = str(ticker or "").split("-")[0].strip().lower()
    return series or "untagged"


def apply_theme_caps(briefs: list, bankroll: float,
                     cap_fraction: float = THEME_CAP_FRACTION) -> list:
    """
    Clamp the Kelly bet on each brief so no theme exceeds cap_fraction of bankroll.

    Greedy by descending bet size: the strongest-edge bet in a theme keeps its
    full size, later bets in the same theme are trimmed to the remaining budget
    (or zeroed). Each brief gets:
      - brief['theme']                      the inferred theme key
      - brief['kelly']['suggested_bet_usd'] possibly reduced
      - brief['kelly']['capped_fraction']   recomputed to match
      - brief['kelly']['guardrails_applied'] appended with a note when trimmed

    Briefs are mutated in place and also returned.
    """
    if not briefs or bankroll <= 0:
        for b in briefs or []:
            b.setdefault("theme", infer_theme(b.get("ticker", ""), b.get("title", "")))
        return briefs

    theme_budget = bankroll * cap_fraction

    for b in briefs:
        b["theme"] = infer_theme(b.get("ticker", ""), b.get("title", ""))

    # Largest bets first so the highest-conviction bet in a theme survives intact.
    order = sorted(
        range(len(briefs)),
        key=lambda i: (briefs[i].get("kelly") or {}).get("suggested_bet_usd", 0.0),
        reverse=True,
    )

    theme_used = {}
    for i in order:
        b = briefs[i]
        kelly = b.get("kelly")
        if not kelly:
            continue
        bet = float(kelly.get("suggested_bet_usd", 0.0) or 0.0)
        if bet <= 0:
            continue

        theme     = b["theme"]
        used      = theme_used.get(theme, 0.0)
        remaining = theme_budget - used

        if remaining <= 0:
            new_bet = 0.0
            note = (f"theme exposure cap: '{theme}' already at "
                    f"${theme_budget:.2f} limit — bet zeroed")
        elif bet > remaining:
            new_bet = round(remaining, 2)
            note = (f"theme exposure cap: '{theme}' trimmed "
                    f"${bet:.2f} -> ${new_bet:.2f} (theme limit ${theme_budget:.2f})")
        else:
            new_bet = bet
            note = None

        theme_used[theme] = used + new_bet

        if note:
            kelly["suggested_bet_usd"] = new_bet
            kelly["capped_fraction"]   = round(new_bet / bankroll, 6) if bankroll else 0.0
            kelly.setdefault("guardrails_applied", []).append(note)
            kelly["reasoning"] = str(kelly.get("reasoning", "")) + " " + note + "."
            if new_bet <= 0:
                kelly["edge_detected"] = False

    return briefs


if __name__ == "__main__":
    demo = [
        {"ticker": "KXPOWELLPROTEMP-26MAY-JUN01",
         "title": "Will Jerome Powell be out as Chair pro tempore before Jun 1, 2026?",
         "kelly": {"suggested_bet_usd": 50.0}},
        {"ticker": "KXLEAVEPOWELLGOV-26AUG01",
         "title": "Will Jerome Powell leave Member of the Board of Governors before Aug 1, 2026?",
         "kelly": {"suggested_bet_usd": 50.0}},
        {"ticker": "KXARREST-27JAN-LJAM",
         "title": "Will Letitia James be arrested before Jan 2027?",
         "kelly": {"suggested_bet_usd": 40.0}},
    ]
    for b in apply_theme_caps(demo, bankroll=1000.0):
        print(b["ticker"], "->", b["theme"], "bet=$%.2f" % b["kelly"]["suggested_bet_usd"])
