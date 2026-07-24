"""enriched_score.py — multi-signal maker opportunity score.

Formula (Section 3 of strategy doc):
    base_opportunity = (event_vol24h * spread_c) / (inside_depth_usd + 50)
    balance_factor   = min(best_bid_sz, best_ask_sz) / max(...)
    fee_factor       = fee_type_map.get(feeType, 0.85)
    neg_risk_factor  = 0.80 if neg_risk else 1.00
    res_factor       = bracket_lookup(days_to_end)
    as_factor         = topic_factor_map.get(topic_key, 0.50)
    enriched_score = base_opportunity
                    * balance_factor * fee_factor * neg_risk_factor
                    * res_factor * as_factor

Factors are calibrated empirically — inputs match config.yaml defaults.
The base_opportunity is the raw scanner metric; the factors downweight
candidates for unbalanced books, fee-heavy categories, neg-risk roll-up,
low resolution-time attraction, or high adverse-selection topics.
"""
from __future__ import annotations
import math
from dataclasses import dataclass, field


DEFAULT_FEE_MAP = {
    "sports_fees_v2": 0.95,
    "finance_prices_fees": 0.90,
    "crypto_fees_v2": 0.90,
    "tech_fees": 0.75,
    "mentions_fees": 0.85,
    "culture_fees": 0.85,
    "default": 0.85,
}

DEFAULT_RES_BRACKETS = [
    (0.25, 2.0),
    (1.0, 1.5),
    (7.0, 1.2),
    (30.0, 1.0),
    (90.0, 0.85),
    (180.0, 0.70),
    (float("inf"), 0.50),
]

DEFAULT_TOPIC_MAP = {
    "politics": 0.25,
    "geopolitics": 0.30,
    "election": 0.25,
    "esports": 0.40,
    "gaming": 0.40,
    "macro": 0.45,
    "commodity": 0.45,
    "crypto": 0.50,
    "weather": 0.70,
    "default": 0.50,
}


@dataclass
class EnrichedScorer:
    fee_map: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_FEE_MAP))
    res_brackets: list[tuple[float, float]] = field(
        default_factory=lambda: list(DEFAULT_RES_BRACKETS)
    )
    topic_map: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_TOPIC_MAP))
    inside_depth_floor_usd: float = 50.0

    def base_opportunity(
        self, event_vol24h_usd: float, spread_c: float, inside_depth_usd: float
    ) -> float:
        denom = max(inside_depth_usd + self.inside_depth_floor_usd, self.inside_depth_floor_usd)
        if denom <= 0 or event_vol24h_usd <= 0 or spread_c <= 0:
            return 0.0
        return (event_vol24h_usd * spread_c) / denom

    def balance_factor(self, best_bid_size: float, best_ask_size: float) -> float:
        a, b = max(best_bid_size, 0.0), max(best_ask_size, 0.0)
        if max(a, b) <= 0:
            return 0.0
        return min(a, b) / max(a, b)

    def fee_factor(self, fee_type: str | None) -> float:
        if not fee_type:
            return self.fee_map["default"]
        return self.fee_map.get(fee_type, self.fee_map["default"])

    def neg_risk_factor(self, neg_risk: bool) -> float:
        return 0.80 if neg_risk else 1.00

    def res_factor(self, days_to_end: float | None) -> float:
        if days_to_end is None or days_to_end < 0:
            return 0.50
        for cap, factor in self.res_brackets:
            if days_to_end <= cap:
                return factor
        return 0.50

    def topic_factor(self, topic_key: str | None) -> float:
        if not topic_key:
            return self.topic_map["default"]
        key = topic_key.lower()
        for k, v in self.topic_map.items():
            if k in key:
                return v
        return self.topic_map["default"]

    def score(
        self,
        *,
        event_vol24h_usd: float,
        spread_c: float,
        inside_depth_usd: float,
        best_bid_size: float,
        best_ask_size: float,
        fee_type: str | None,
        neg_risk: bool,
        days_to_end: float | None,
        topic_key: str | None,
    ) -> dict:
        base = self.base_opportunity(event_vol24h_usd, spread_c, inside_depth_usd)
        bf = self.balance_factor(best_bid_size, best_ask_size)
        ff = self.fee_factor(fee_type)
        nrf = self.neg_risk_factor(neg_risk)
        rf = self.res_factor(days_to_end)
        af = self.topic_factor(topic_key)
        enriched = base * bf * ff * nrf * rf * af
        return {
            "base_opportunity": base,
            "balance_factor": bf,
            "fee_factor": ff,
            "neg_risk_factor": nrf,
            "res_factor": rf,
            "as_factor": af,
            "enriched_score": enriched,
        }


def infer_topic_key(text: str | None) -> str:
    """Coarse classifier for AS-factor lookup.
    Polymarket event/meta fields often carry keywords that let us pick
    a category; if absent we return 'default'."""
    if not text:
        return "default"
    t = text.lower()
    if any(k in t for k in ("bitcoin", "btc", "ethereum", "eth", "solana", "crypto", "altcoin")):
        return "crypto"
    if any(k in t for k in ("election", "president", "congress", "senate", "governor", "prime minister", "cabinet")):
        return "politics"
    if any(k in t for k in ("iran", "russia", "china", "war", "invasion", "geopolit", "airtor", "airspace")):
        return "geopolitics"
    if any(k in t for k in ("league", "valorant", "lpl", "lck", "esports", "gaming", "match", "vs")):
        return "esports"
    if any(k in t for k in ("oil", "crude", "gold", "silver", "wheat", "natgas", "commodity")):
        return "commodity"
    if any(k in t for k in ("cpi", "gdp", "fed", "rate", "inflation", "employ", "jobs", "nfp")):
        return "macro"
    if any(k in t for k in ("temp", "hurricane", "snowfall", "rainfall", "weather")):
        return "weather"
    return "default"


def days_to_end(end_date_str: str | None, now_dt=None) -> float | None:
    """Return days between now and end_date (or None if unparseable).
    Accepts ISO date strings like '2026-08-01' or full datetime."""
    if not end_date_str:
        return None
    import datetime as _dt
    if now_dt is None:
        now_dt = _dt.datetime.now(tz=_dt.timezone.utc)
    s = end_date_str.strip().rstrip("Z")
    fmts = ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d")
    parsed = None
    for fmt in fmts:
        try:
            if fmt.endswith("%f"):
                parsed = _dt.datetime.strptime(s, fmt)
            else:
                parsed = _dt.datetime.strptime(s, fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_dt.timezone.utc)
    delta = (parsed - now_dt).total_seconds() / 86400.0
    return delta
