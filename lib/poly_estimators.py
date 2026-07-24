"""poly_estimators.py — port of poly-maker/strategy/estimators.py (time-decayed EWMAs).

All estimators are PURE STATE MACHINES — zero I/O, zero log. Feed them (value, ts)
observations; read scalar summaries. The time-decay is by elapsed wall-clock, not
sample count, so a burst of ticks and a quiet minute are weighted correctly.

Phases:
  - Ewma            : time-decayed exponentially weighted mean
  - VolEstimator    : realized vol at two horizons from fair-value changes
  - FlowEstimator   : signed aggressor flow + normalized strength (z-score shape)
  - MarkoutTracker  : per-fill adverse selection measurement at horizon_s
  - MarketEstimators: bundle of vol + flow + markout for a single market
"""
from __future__ import annotations
import math
from dataclasses import dataclass
from enum import Enum


class Side(Enum):
    BUY = 1
    SELL = 2


class Ewma:
    """Time-decayed exponentially weighted mean (half-life in seconds)."""
    __slots__ = ("halflife", "_value", "_last_ts", "_initialized")

    def __init__(self, halflife_s: float) -> None:
        if halflife_s <= 0:
            raise ValueError("halflife must be positive")
        self.halflife = halflife_s
        self._value = 0.0
        self._last_ts = 0.0
        self._initialized = False

    def update(self, value: float, ts: float) -> float:
        if not self._initialized:
            self._value = value
            self._last_ts = ts
            self._initialized = True
            return self._value
        dt = max(0.0, ts - self._last_ts)
        decay = 0.5 ** (dt / self.halflife)
        self._value = decay * self._value + (1.0 - decay) * value
        self._last_ts = ts
        return self._value

    def decay_to(self, ts: float) -> float:
        """Decay stored value toward 0 as if observing 0 at `ts`."""
        if self._initialized:
            dt = max(0.0, ts - self._last_ts)
            self._value *= 0.5 ** (dt / self.halflife)
            self._last_ts = ts
        return self._value

    @property
    def value(self) -> float:
        return self._value

    @property
    def ready(self) -> bool:
        return self._initialized


class VolEstimator:
    """Realized volatility at two horizons from fair-value changes."""
    __slots__ = ("_short", "_long", "_last_fv", "_last_ts")

    def __init__(self, short_halflife_s: float = 30.0, long_halflife_s: float = 600.0) -> None:
        self._short = Ewma(short_halflife_s)
        self._long = Ewma(long_halflife_s)
        self._last_fv: float | None = None
        self._last_ts = 0.0

    def update(self, fv: float, ts: float) -> None:
        if self._last_fv is not None:
            r = fv - self._last_fv
            sq = r * r
            self._short.update(sq, ts)
            self._long.update(sq, ts)
        self._last_fv = fv
        self._last_ts = ts

    @property
    def short(self) -> float:
        return math.sqrt(max(0.0, self._short.value))

    @property
    def long(self) -> float:
        return math.sqrt(max(0.0, self._long.value))

    @property
    def ratio(self) -> float:
        lo = self.long
        return self.short / lo if lo > 1e-9 else 1.0


class FlowEstimator:
    """Signed aggressor flow + z-score-style normalized strength."""
    __slots__ = ("_signed", "_abs")

    def __init__(self, halflife_s: float = 5.0) -> None:
        self._signed = Ewma(halflife_s)
        self._abs = Ewma(halflife_s)

    def update(self, aggressor: Side, size: float, ts: float) -> None:
        signed = size if aggressor is Side.BUY else -size
        self._signed.update(signed, ts)
        self._abs.update(abs(size), ts)

    def decay_to(self, ts: float) -> None:
        self._signed.decay_to(ts)
        self._abs.decay_to(ts)

    @property
    def signed(self) -> float:
        return self._signed.value

    @property
    def z(self) -> float:
        denom = self._abs.value
        return self._signed.value / denom if denom > 1e-9 else 0.0


@dataclass(slots=True)
class _PendingMarkout:
    fv_at_fill: float
    side: Side  # our side of the fill (BUY → adverse if price falls)
    due_ts: float


class MarkoutTracker:
    """Per-fill adverse selection measurement at horizon_s; tracks the EWMA of
    post-fill (fv_now - fv_at_fill), signed so that POSITIVE = good fill, NEGATIVE =
    picked off. Toxicity = magnitude of recent NEGATIVE markout.
    """
    __slots__ = ("_horizon_s", "_pending", "_markout")

    def __init__(self, horizon_s: float = 300.0, ewma_halflife_s: float = 1800.0) -> None:
        self._horizon_s = horizon_s
        self._pending: list[_PendingMarkout] = []
        self._markout = Ewma(ewma_halflife_s)

    def record_fill(self, side: Side, fv_at_fill: float, ts: float) -> None:
        self._pending.append(_PendingMarkout(fv_at_fill, side, ts + self._horizon_s))

    def evaluate(self, fv_now: float, ts: float) -> None:
        still: list[_PendingMarkout] = []
        for p in self._pending:
            if ts >= p.due_ts:
                move = fv_now - p.fv_at_fill
                signed = move if p.side is Side.BUY else -move
                self._markout.update(signed, ts)
            else:
                still.append(p)
        self._pending = still

    @property
    def markout(self) -> float:
        return self._markout.value

    @property
    def toxicity(self) -> float:
        return max(0.0, -self._markout.value)


@dataclass
class MarketEstimators:
    """Bundle of per-market online estimators that the engine keeps."""
    vol: VolEstimator
    flow: FlowEstimator
    markout: MarkoutTracker
    last_fv: float | None = None
    last_fv_ts: float = 0.0

    def on_fair_value(self, fv: float, ts: float) -> None:
        self.vol.update(fv, ts)
        self.markout.evaluate(fv, ts)
        self.last_fv = fv
        self.last_fv_ts = ts


def market_estimator_default() -> MarketEstimators:
    return MarketEstimators(
        vol=VolEstimator(),
        flow=FlowEstimator(),
        markout=MarkoutTracker(),
    )
