"""
PredictIQ - Kelly Criterion Engine
====================================
Calculates mathematically optimal bet sizing per market using the
Kelly Criterion formula: f = W - (1-W)/R
where:
  W = Win probability (derived from AI confidence_score)
  R = Reward-to-Risk ratio (derived from Kalshi yes_bid odds)
"""

def calculate_kelly(confidence_score: float, yes_bid: float, bankroll: float) -> dict:
    """
    Args:
        confidence_score: AI confidence 0.0 - 1.0 (e.g. 0.82 from Groq)
        yes_bid:          Kalshi current yes bid (0.0 - 1.0 scale, e.g. 0.30 = 30¢)
        bankroll:         Total available cash balance in dollars.
    Returns:
        dict with kelly_fraction, suggested_bet_usd, capped_fraction, reasoning
    """
    if yes_bid <= 0 or yes_bid >= 1:
        return _zero_bet("Invalid odds: yes_bid must be between 0 and 1")
    if confidence_score <= 0:
        return _zero_bet("No AI confidence signal — do not bet")
    if bankroll <= 0:
        return _zero_bet("Bankroll is zero")

    # W = AI-implied win probability
    W = float(confidence_score)

    # On Kalshi, if yes_bid = 0.30, you risk $0.30 to win $0.70.
    # Reward-to-Risk Ratio: how much you win per dollar risked
    payout_per_dollar = (1.0 - yes_bid) / yes_bid  # e.g. 0.70/0.30 = 2.33
    R = payout_per_dollar

    # Full Kelly Fraction
    kelly_fraction = W - ((1.0 - W) / R)

    # Negative Kelly = no edge, do not bet
    if kelly_fraction <= 0:
        return _zero_bet(f"No edge detected (Kelly={kelly_fraction:.4f}). Market is fairly priced vs AI confidence.")

    # Quarter-Kelly: industry standard for prediction markets (reduces variance)
    quarter_kelly = kelly_fraction * 0.25

    # Hard cap at 5% of bankroll per trade (risk management guardrail)
    HARD_CAP = 0.05
    capped_fraction = min(quarter_kelly, HARD_CAP)

    suggested_bet = round(bankroll * capped_fraction, 2)

    return {
        "kelly_fraction": round(kelly_fraction, 4),
        "quarter_kelly_fraction": round(quarter_kelly, 4),
        "capped_fraction": round(capped_fraction, 4),
        "suggested_bet_usd": suggested_bet,
        "edge_detected": True,
        "reasoning": (
            f"W={W:.0%} win prob vs R={R:.2f}x payout. "
            f"Full Kelly={kelly_fraction:.1%}, Quarter-Kelly={quarter_kelly:.1%}, "
            f"capped at {HARD_CAP:.0%}. Suggested: ${suggested_bet:.2f} of ${bankroll:.2f} bankroll."
        )
    }


def _zero_bet(reason: str) -> dict:
    return {
        "kelly_fraction": 0.0,
        "quarter_kelly_fraction": 0.0,
        "capped_fraction": 0.0,
        "suggested_bet_usd": 0.0,
        "edge_detected": False,
        "reasoning": reason
    }


# Quick test
if __name__ == "__main__":
    result = calculate_kelly(confidence_score=0.72, yes_bid=0.30, bankroll=1000.0)
    print(result)
