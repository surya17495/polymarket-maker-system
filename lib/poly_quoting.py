"""poly_quoting.py — adapted port of poly-maker/strategy/quoting.py.

Pure construction: (book state, inventory, params) → list of QuoteSpec.

Model (per poly-maker README):
  reservation  r  = FV − skew(inventory)
  half-spread  δ  = base + c_vol·σ + c_tox·toxicity   (clamped to reward band in QUIET)
  YES entry bid  = r − δ          (BUY YES, USDC-collateralized)
  NO  entry bid  = (1 − r) − δ    (BUY NO; implied YES ask at r + δ)
  exits          = SELL limits on held inventory, walked toward touch by urgency

This module emits "QuoteSpec" dataclasses (token_id, side, price, size); the
Strategy wrapper (lib/strategies.py) converts these to QuoteSubmit events against our
BookStore + InventoryState.

NO-book derivation: when raw_events.jsonl only has the YES side (subscribed to
yes_token only), the NO book is derived from YES via the symmetry
  NO best_bid = 1 − YES best_ask          (size mirrored)
  NO best_ask = 1 − YES best_bid          (size mirrored)
which is exact for Polymarket's binary YES+NO=$1 contract.
"""
from __future__ import annotations
import math
from dataclasses import dataclass
from decimal import Decimal

from lib.poly_regime import Regime


_EPS = 1e-9


def round_to_tick(price: float, tick: float, decimals: int, *, up: bool) -> float:
    """Snap a price to the tick grid, rounding up or down, clamped to (0, 1)."""
    n = price / tick
    n = math.ceil(n - _EPS) if up else math.floor(n + _EPS)
    p = round(n * tick, decimals)
    return min(max(p, tick), 1.0 - tick)


def compute_fair_value(microprice: float, flow_z: float, tick: float, weight: float = 0.5) -> float:
    """Nudge microprice by bounded signed flow. Clamped to (tick, 1 − tick)."""
    fv = microprice + weight * flow_z * tick
    return min(max(fv, tick), 1.0 - tick)


@dataclass(frozen=True, slots=True)
class QuoteInputs:
    fv: float                 # YES fair value in (0, 1)
    vol_short: float          # realized short-horizon vol (sqrt EWMA of (ΔFV)^2)
    toxicity: float           #_MARKOUT sign-flipped, ≥ 0
    regime: Regime
    bb_yes: Decimal | None
    ba_yes: Decimal | None
    bb_no: Decimal | None     # None → derive from YES via symmetry
    ba_no: Decimal | None     # None → derive from YES
    bb_yes_size: Decimal
    ba_yes_size: Decimal
    bb_no_size: Decimal | None
    ba_no_size: Decimal | None
    pos_yes_shares: float     # long inventory in YES
    pos_no_shares: float     # long inventory in NO
    tick_size: float
    price_decimals: int = 4
    # profile params (mirrors poly-maker StrategyProfile):
    gamma: float = 1.0
    delta_min_ticks: int = 1
    c_vol: float = 5.0
    c_tox: float = 2.0
    q_max_usdc: float = 200.0
    q_soft_frac: float = 0.7
    base_size_usdc: float = 50.0
    reward_size_mult: float = 1.0
    min_edge_ticks: int = 1
    layers: int = 1
    layer_step_ticks: int = 1
    min_order_size: float = 0.0
    reward_floor: float = 0.0
    rewards_max_spread: float = 0.0   # reward band; 0 disables band clamp
    yes_exit_urgency: float = 0.0
    no_exit_urgency: float = 0.0
    risk_size_scale: float = 1.0
    yes_token_id: str = ""
    no_token_id: str = ""
    quote_market: str = ""             # condition_id (shared by YES+NO)


@dataclass(frozen=True, slots=True)
class QuoteSpec:
    """A pure quote spec — Strategy wrapper converts to QuoteSubmit."""
    token_id: str
    side: str         # "BUY" or "SELL"
    price: float
    size: float       # shares


def _clamp(x: float, lo: float, hi: float) -> float:
    return min(max(x, lo), hi)


def _place_bid(
    target: float,
    bb: Decimal | None,
    ba: Decimal | None,
    tick: float,
    dec: int,
    fv: float,
    min_edge_ticks: int,
) -> float | None:
    """Position a BUY: join the touch or sit behind, never cross, keep min edge vs FV."""
    price = target
    price = min(price, fv - min_edge_ticks * tick)
    if bb is not None and price >= float(bb):
        price = float(bb)
    if ba is not None and price >= float(ba):
        price = float(ba) - tick
    p = round_to_tick(price, tick, dec, up=False)
    if p <= 0 or p >= 1:
        return None
    return p


def _size_shares(base_usdc: float, price: float, scale: float, min_order_size: float) -> float:
    if price <= 0:
        return 0.0
    shares = (base_usdc / price) * max(scale, 0.0)
    if shares < min_order_size:
        return 0.0
    return round(shares, 2)


def _maybe_exit(
    pos_size: float,
    token_fv: float,
    delta: float,
    bb: Decimal | None,
    tick: float,
    dec: int,
    urgency: float,
    regime: Regime,
    min_order_size: float,
    token_id: str,
    quote_market: str,
) -> QuoteSpec | None:
    if pos_size < min_order_size or pos_size <= 0:
        return None
    passive = token_fv + delta
    floor = (float(bb) + tick) if bb is not None else passive
    if regime == Regime.REDUCE_ONLY:
        urgency = max(urgency, 0.5)
    target = passive * (1.0 - urgency) + floor * urgency
    if bb is not None:
        target = max(target, float(bb) + tick)
    price = round_to_tick(target, tick, dec, up=True)
    size = math.floor(pos_size * 100) / 100
    if 0 < price < 1 and size >= min_order_size:
        return QuoteSpec(token_id=token_id, side="SELL", price=price, size=size)
    return None


def construct_quotes(inp: QuoteInputs) -> list[QuoteSpec]:
    """Pure construction: return list of BUY/SELL QuoteSpec entries + exits per asset."""
    tick = inp.tick_size
    dec = inp.price_decimals
    if inp.regime in (Regime.EVENT, Regime.HALTED):
        return []
    quotes: list[QuoteSpec] = []
    net_shares = inp.pos_yes_shares - inp.pos_no_shares
    q_max_shares = inp.q_max_usdc / max(inp.fv, tick)
    u = _clamp(net_shares / q_max_shares, -1.0, 1.0) if q_max_shares > 0 else 0.0
    reward_floor = max(inp.reward_floor, inp.min_order_size) * inp.reward_size_mult

    skew = inp.gamma * inp.vol_short * u
    base = inp.delta_min_ticks * tick
    delta = base + inp.c_vol * inp.vol_short + inp.c_tox * inp.toxicity
    if inp.regime == Regime.QUIET and inp.rewards_max_spread > 0:
        reward_band = inp.rewards_max_spread / 100.0
        delta = _clamp(delta, base, max(base, reward_band))
    delta = max(delta, tick)

    r = inp.fv - skew
    yes_bid_target = r - delta
    no_bid_target = (1.0 - r) - delta

    # Derive NO book from YES via symmetry when not supplied
    bb_no = inp.bb_no if inp.bb_no is not None and float(inp.bb_no) > 0 else (
        Decimal("1") - (inp.ba_yes or Decimal(0)) if (inp.ba_yes is not None) else None
    )
    ba_no = inp.ba_no if inp.ba_no is not None and float(inp.ba_no) > 0 else (
        Decimal("1") - (inp.bb_yes or Decimal(0)) if (inp.bb_yes is not None) else None
    )
    bb_no_size = inp.bb_no_size if inp.bb_no_size is not None else inp.ba_yes_size
    ba_no_size = inp.ba_no_size if inp.ba_no_size is not None else inp.bb_yes_size

    regime_scale = 0.5 if inp.regime == Regime.TRENDING else 1.0
    tox_scale = 1.0 / (1.0 + inp.toxicity * 10.0)
    common_scale = regime_scale * tox_scale * _clamp(inp.risk_size_scale, 0.0, 1.0)

    add_yes = inp.regime not in (Regime.REDUCE_ONLY,) and u < inp.q_soft_frac
    add_no = inp.regime not in (Regime.REDUCE_ONLY,) and u > -inp.q_soft_frac

    if add_yes:
        price = _place_bid(yes_bid_target, inp.bb_yes, inp.ba_yes, tick, dec, inp.fv, inp.min_edge_ticks)
        if price is not None:
            size = _size_shares(inp.base_size_usdc, price, common_scale * (1 - max(u, 0.0)), inp.min_order_size)
            if 0 < size and (size >= reward_floor if reward_floor > 0 else True):
                quotes.append(QuoteSpec(token_id=inp.yes_token_id, side="BUY", price=price, size=size))

    if add_no:
        no_fv = 1.0 - inp.fv
        price = _place_bid(no_bid_target, bb_no, ba_no, tick, dec, no_fv, inp.min_edge_ticks)
        if price is not None:
            size = _size_shares(inp.base_size_usdc, price, common_scale * (1 - max(-u, 0.0)), inp.min_order_size)
            if 0 < size and (size >= reward_floor if reward_floor > 0 else True):
                quotes.append(QuoteSpec(token_id=inp.no_token_id, side="BUY", price=price, size=size))

    # exits: sell held inventory (maker, never cross)
    ex_yes = _maybe_exit(
        inp.pos_yes_shares, inp.fv, delta, inp.bb_yes, tick, dec,
        inp.yes_exit_urgency, inp.regime, inp.min_order_size,
        inp.yes_token_id, inp.quote_market,
    )
    if ex_yes:
        quotes.append(ex_yes)
    no_fv = 1.0 - inp.fv
    ex_no = _maybe_exit(
        inp.pos_no_shares, no_fv, delta, bb_no, tick, dec,
        inp.no_exit_urgency, inp.regime, inp.min_order_size,
        inp.no_token_id, inp.quote_market,
    )
    if ex_no:
        quotes.append(ex_no)
    return quotes
