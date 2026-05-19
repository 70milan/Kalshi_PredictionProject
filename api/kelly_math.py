"""
PredictIQ - Kelly Criterion Engine
====================================
Calculates mathematically optimal bet sizing per market using the
Kelly Criterion formula: f = W - (1-W)/R
where:
  W = Win probability (derived from AI confidence_score)
  R = Reward-to-Risk ratio (derived from Kalshi yes_bid odds)

Risk guardrails layered on top of raw Kelly (added 2026-05-18 after the
KXPOWELLPROTEMP loss — a single uncalibrated bet sized to the hard cap):
  A. Calibration cap   — until enough markets resolve, every bet is clamped flat.
  B. Confidence shrink — raw AI confidence is pulled toward 0.5 before it is
                         treated as a win probability (Kelly assumes W is the
                         TRUE probability; raw LLM confidence is overstated).
  E. Short-dated haircut — markets resolving soon get a reduced per-trade cap.
  F. Confidence floor  — below the floor, bets are clamped flat (no upsizing).
"""

# ── Risk guardrail constants ──────────────────────────────
QUARTER_KELLY            = 0.25   # fraction of full Kelly to bet (variance control)
HARD_CAP                 = 0.05   # max fraction of bankroll on any single trade
FLAT_BET_USD             = 10.0   # bet size while uncalibrated / low-confidence

# B — confidence shrink: W_adj = 0.5 + SHRINK * (conf - 0.5)
CONFIDENCE_SHRINK        = 0.5    # 70% -> 60%, 80% -> 65%, 60% -> 55%

# F — confidence floor: below this raw confidence, no Kelly upsizing
CONFIDENCE_FLOOR         = 0.70

# A — calibration: resolved-market count required before trusting Kelly sizing
MIN_CALIBRATION_SAMPLES  = 20

# E — short-dated markets: resolving within this many days get a cap haircut
SHORT_DATED_DAYS         = 7
SHORT_DATED_HAIRCUT      = 0.5    # multiply the per-trade cap by this


def calculate_kelly(confidence_score: float, yes_bid: float, bankroll: float,
                    *, resolved_count: int = None,
                    days_to_resolution: float = None) -> dict:
    """
    Args:
        confidence_score:   AI confidence 0.0 - 1.0 (e.g. 0.82 from the LLM).
        yes_bid:            Price of the contract being bought, 0.0 - 1.0.
                            For a NO bet the caller must pass (1 - yes_bid).
        bankroll:           Total available cash balance in dollars.
        resolved_count:     Number of markets resolved so far (calibration sample).
                            None = skip the calibration cap.
        days_to_resolution: Days until the market settles. None = skip the
                            short-dated haircut.
    Returns:
        dict with kelly_fraction, suggested_bet_usd, capped_fraction,
        guardrails_applied, reasoning, and diagnostics.
    """
    if yes_bid <= 0 or yes_bid >= 1:
        return _zero_bet("Invalid odds: yes_bid must be between 0 and 1")
    if confidence_score <= 0:
        return _zero_bet("No AI confidence signal — do not bet")
    if bankroll <= 0:
        return _zero_bet("Bankroll is zero")

    raw_conf = float(confidence_score)

    # ── B: shrink confidence toward 0.5 before it becomes a win probability ──
    # Kelly assumes W is the TRUE probability. Raw LLM confidence is uncalibrated
    # and overstated, so we pull it halfway toward a coin flip before sizing.
    W = 0.5 + CONFIDENCE_SHRINK * (raw_conf - 0.5)

    # On Kalshi, if yes_bid = 0.30, you risk $0.30 to win $0.70.
    # Reward-to-Risk Ratio: how much you win per dollar risked.
    payout_per_dollar = (1.0 - yes_bid) / yes_bid  # e.g. 0.70/0.30 = 2.33
    R = payout_per_dollar

    # Full Kelly Fraction (computed on the SHRUNK win probability)
    kelly_fraction = W - ((1.0 - W) / R)

    # Negative Kelly = no edge after shrink, do not bet
    if kelly_fraction <= 0:
        return _zero_bet(
            f"No edge after confidence shrink (W={W:.0%} from raw {raw_conf:.0%}, "
            f"Kelly={kelly_fraction:.4f}). Not enough vs {R:.2f}x payout.",
            raw_conf=raw_conf, adjusted_win_prob=W,
        )

    # Quarter-Kelly: industry standard for prediction markets (reduces variance)
    quarter_kelly = kelly_fraction * QUARTER_KELLY

    # ── E: short-dated haircut — per-day headline risk is high near resolution ──
    cap = HARD_CAP
    short_dated = days_to_resolution is not None and days_to_resolution < SHORT_DATED_DAYS
    if short_dated:
        cap = HARD_CAP * SHORT_DATED_HAIRCUT

    capped_fraction = min(quarter_kelly, cap)
    suggested_bet   = round(bankroll * capped_fraction, 2)

    # ── A + F: clamp to a flat bet while the model is unproven / low-confidence ──
    # Kelly's "optimal" sizing is only valid once W is calibrated. Until then,
    # and whenever raw confidence is below the floor, never bet above the flat
    # amount — this is what stops one trade from dominating the book.
    guardrails = []
    uncalibrated   = resolved_count is not None and resolved_count < MIN_CALIBRATION_SAMPLES
    low_confidence = raw_conf < CONFIDENCE_FLOOR
    if short_dated:
        guardrails.append(f"short-dated haircut (resolves in {days_to_resolution:.0f}d, cap halved)")
    if uncalibrated or low_confidence:
        if uncalibrated:
            guardrails.append(
                f"calibration cap ({resolved_count}/{MIN_CALIBRATION_SAMPLES} markets resolved)"
            )
        if low_confidence:
            guardrails.append(
                f"confidence floor (raw {raw_conf:.0%} < {CONFIDENCE_FLOOR:.0%})"
            )
        suggested_bet   = min(suggested_bet, FLAT_BET_USD)
        capped_fraction = round(suggested_bet / bankroll, 6) if bankroll else 0.0

    reasoning = (
        f"W={W:.0%} (shrunk from raw {raw_conf:.0%}) vs R={R:.2f}x payout. "
        f"Full Kelly={kelly_fraction:.1%}, Quarter-Kelly={quarter_kelly:.1%}. "
        f"Suggested: ${suggested_bet:.2f} of ${bankroll:.2f} bankroll."
    )
    if guardrails:
        reasoning += " Guardrails: " + "; ".join(guardrails) + "."

    return {
        "kelly_fraction":         round(kelly_fraction, 4),
        "quarter_kelly_fraction": round(quarter_kelly, 4),
        "capped_fraction":        round(capped_fraction, 4),
        "suggested_bet_usd":      suggested_bet,
        "edge_detected":          True,
        "raw_confidence":         round(raw_conf, 4),
        "adjusted_win_prob":      round(W, 4),
        "short_dated":            short_dated,
        "guardrails_applied":     guardrails,
        "reasoning":              reasoning,
    }


def _zero_bet(reason: str, raw_conf: float = 0.0, adjusted_win_prob: float = 0.0) -> dict:
    return {
        "kelly_fraction":         0.0,
        "quarter_kelly_fraction": 0.0,
        "capped_fraction":        0.0,
        "suggested_bet_usd":      0.0,
        "edge_detected":          False,
        "raw_confidence":         round(raw_conf, 4),
        "adjusted_win_prob":      round(adjusted_win_prob, 4),
        "short_dated":            False,
        "guardrails_applied":     [],
        "reasoning":              reason,
    }


# Quick test
if __name__ == "__main__":
    print("Calibrated, high confidence:")
    print(calculate_kelly(0.82, 0.30, 1000.0, resolved_count=50))
    print("\nUncalibrated (0 resolved) — should clamp to $10:")
    print(calculate_kelly(0.82, 0.30, 1000.0, resolved_count=0))
    print("\nLow confidence (60%) — should clamp to $10:")
    print(calculate_kelly(0.60, 0.45, 1000.0, resolved_count=50))
    print("\nShort-dated (3 days) — halved cap:")
    print(calculate_kelly(0.82, 0.30, 1000.0, resolved_count=50, days_to_resolution=3))
