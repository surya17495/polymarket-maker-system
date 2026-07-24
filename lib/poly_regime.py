"""poly_regime.py — port of poly-maker/strategy/regime.py.

5-state machine for a single market. Edge inputs:
  - now:                wall-clock ts (s)
  - tick:               tick_size (decimal)
  - fv:                 current fair value
  - prev_fv:            last-tick fair value (for jump detection)
  - vol_ratio:          short/long realized-vol ratio (>1 = recent activity surge)
  - flow_z:            signed-flow z-score
  - inventory_util:     |net notional| / q_max, ≥0
  - hours_to_end:       time until market resolves (None = perpetual)
  - sweep_flagged:      True if SweepDetector triggered
  - market_resolved:    True when book indicates market resolved
  - ws_stale:           True when WS is too stale to trust
  - risk_halt:          account-level halt (drawdown or hard cap)
  - risk_reduce_only:   account-level reduce-only flag

Priority (highest first):
  HALTED      kill switch / stale WS / resolved / past halt-before window
  EVENT       active cool-off, or fresh sweep / fair-value jump
  REDUCE_ONLY inventory at hard cap, or inside the reduce-only end window
  TRENDING    persistent one-sided flow or elevated short/long vol ratio
  QUIET       default farming posture
"""
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum


class Regime(Enum):
    HALTED = 0
    EVENT = 1
    REDUCE_ONLY = 2
    TRENDING = 3
    QUIET = 4


@dataclass(slots=True)
class StrategyProfile:
    halt_before_hours: float = 0.25  # was 1.0 -> too aggressive; kills esports markets whose endDate is within 1h of capture moment, blocking all quotes. Tighten to 15-minutes-of-resolution stop-work.
    reduce_only_hours: float = 12.0
    event_jump_ticks: int = 30      # FV jump > 30 ticks = event
    event_cooloff_s: float = 30.0
    trend_flow_z: float = 0.6       # |flow_z| ≥ 0.6 = trending
    trend_vol_ratio: float = 1.5   # vol_ratio ≥ 1.5 = trending


@dataclass(frozen=True, slots=True)
class RegimeInputs:
    now: float
    tick: float
    fv: float
    prev_fv: float | None
    vol_ratio: float
    flow_z: float
    inventory_util: float
    hours_to_end: float | None = None
    sweep_flagged: bool = False
    market_resolved: bool = False
    ws_stale: bool = False
    risk_halt: bool = False
    risk_reduce_only: bool = False


class RegimeMachine:
    """Stateful regime decider for one market (tracks the EVENT cooloff window)."""
    __slots__ = ("_event_until",)

    def __init__(self) -> None:
        self._event_until: float = 0.0

    def decide(self, inp: RegimeInputs, p: StrategyProfile) -> Regime:
        if inp.risk_halt or inp.ws_stale or inp.market_resolved:
            return Regime.HALTED
        if inp.hours_to_end is not None and inp.hours_to_end <= p.halt_before_hours:
            return Regime.HALTED

        jump_ticks = abs(inp.fv - inp.prev_fv) / inp.tick if inp.prev_fv is not None and inp.tick > 0 else 0.0
        if inp.sweep_flagged or jump_ticks >= p.event_jump_ticks:
            self._event_until = inp.now + p.event_cooloff_s
            return Regime.EVENT
        if inp.now < self._event_until:
            return Regime.EVENT

        if inp.risk_reduce_only or inp.inventory_util >= 1.0:
            return Regime.REDUCE_ONLY
        if inp.hours_to_end is not None and inp.hours_to_end <= p.reduce_only_hours:
            return Regime.REDUCE_ONLY

        if abs(inp.flow_z) >= p.trend_flow_z or inp.vol_ratio >= p.trend_vol_ratio:
            return Regime.TRENDING

        return Regime.QUIET

    @property
    def in_cooloff(self) -> bool:
        return self._event_until > 0.0

    def cooloff_remaining(self, now: float) -> float:
        return max(0.0, self._event_until - now)
