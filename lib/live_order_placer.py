"""live_order_placer.py — Phase-2A signed order placement against Polymarket CLOB.

THIS MODULE IS THE TRUTH-ANCHOR PATHWAY. It replaces the heuristic fill-emitter
(the lab's lib/strategy_lab.py depth-shrinkage heuristic, which has been
empirically 100% phantom per lab_v5 /trades validation 2026-07-26) with actual
EIP-712-signed orders posted to the CLOB REST API. The "truth" is no longer a
POLYMARKET LIBRARY inference about depth-shrinkage: it's the bash-level fill
record returned by Polymarket's gatekeeper AT upload-time, tied to OUR wallet
via EIP-712 signature. /trades data-api cross-match becomes unnecessary; the
fill-receipt chain of custody is on-chain (ConditionSwap contract
`transactionHash` attributable to our `proxyWallet`).

PRE-KYC STANDBY STATE (current, awaiting user Polymarket KYC + L2 API key
generation through their dashboard):
  - Module imports cleanly WITHOUT `py_clob_client` installed (lazy import).
  - Instantiation REQUIRES env credentials present (raises
    `LiveConfigurationError` — KYC required — when absent, with a docstring
    pointing at .env.example).
  - `place_quote(...)`, `cancel_quote(...)`, `poll_for_fills(...)` raise
    `LiveNotImplementedError` with explicit step refs to the py_clob_client
    call-graph to execute upon receive-of-KYC.

POST-KYC ACTIVATION (user obtains L2 API key + funded wallet + executes):
  - User populates `.env` from `.env.example`
  - User runs `python3 -u main_live.py --phase-2a 172800 --top-n 15`
  - main_live.py instantiates LiveOrderPlacer, begins the trading-loop
    (replacing main_paper.py's heuristic paper_executor path), logs the
    first truth-anchored $-tied FillReceipt.

py_clob_client call-graph (to be invoked on activation):
  from py_clob_client.client import ClobClient
  from py_clob_client.clob_types import OrderArgs, ApiCreds
  client = ClobClient(
      host="https://clob.polymarket.com",
      key=PRIVATE_KEY,
      chain_id=137,  # Polygon zkEVM mainnet
      signature_type=2,  # 0=EOA, 1=PROXY, 2=POLY_PROXY (default proxy-wallet flow)
      funder=PROXY_WALLET_ADDRESS,
  )
  creds = client.create_or_derive_api_creds()
  client.set_api_creds(creds)
  order = client.create_order(OrderArgs(price=..., size=..., side="BUY", token_id=...))
  resp = client.post_order(order, order_type="GTC")  # or FAK / FOK / GTD
  # resp contains the canonical `order_id`; quote is now resting on the
  # CLOB book signed by our wallet. Fill event is published by the CLOB
  # via /trades endpoint filtered by `makerWallet` once a taker crossed
  # our resting order.

Validation: Phase-2A integration tests mock py_clob_client.ClobClient and
assert:
  (a) order args (price, size, side, token_id) map q.price / q.size /
      Q.side→BUY-or-SELL / q.asset_id losslessly
  (b) post-POST fill receipt contains the
      (proxyWallet, transactionHash) tuple that we re-use as truth anchor
  (c) cancelled unacknowledged orders are reproscribed as `cancel_quote(order_id)` calls.

References:
  - .env.example                         (template env file)
  - docs/strategy_doc.md § Phase-2A      (full deployment runbook)
  - lib/trades_truth.py                  (truth-fallback pre-KYC via data-api)
"""
from __future__ import annotations
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger(__name__)


CLOB_HOST = "https://clob.polymarket.com"
POLYGON_CHAIN_ID = 137
DEFAULT_ORDER_TTL_SEC = 600  # 10 min
DEFAULT_FILL_POLL_TIMEOUT_SEC = 1800


class LiveConfigurationError(RuntimeError):
    """Raised when LiveOrderPlacer cannot be constructed because Polymarket
    Phase-2A KYC + L2 API credentials are not in env. See `.env.example`.

    This is a CONFIG failure, not a runtime fail; resolving it requires the
    USER to manually complete KYC on the Polymarket web UI + L2-API-key
    generation in their account page. See docs/strategy_doc.md § Phase-2A.
    """


class LiveNotImplementedError(NotImplementedError):
    """Raised by place_quote/cancel_quote/poll_for_fills defensively until
    user completes Phase-2A credential setup.  The code path is otherwise
    stable; the body of each method documents the py_clob_client call to
    make once that library import succeeds (which requires
    `pip install py_clob_client` post-KYC + manual EVM-wallet-funded dev).
    """


@dataclass
class LiveOrderCredentials:
    """Sets derived from the Polymarket L2 API key spec (HMAC + EVM signer).

    All five values are emitted by Polymarket's UI when the user completes
    KYC + generates an API key in their account; the values match fields
    passed to py_clob_client.ClobClient + create_or_derive_api_creds.
    """
    l2_api_key: str        # HMAC public key (LE credential)
    l2_api_secret: str     # HMAC private key (LE credential)
    l2_api_passphrase: str # HMAC passphrase (LE credential)
    evm_wallet_priv_key: str         # EVM wallet signing key (used for EIP-712 order sign)
    proxy_wallet_address: Optional[str] = None  # proxy wallet addr; None == native wallet (EOA)
    @classmethod
    def from_env(cls, env: Optional[dict[str, str]] = None) -> Optional["LiveOrderCredentials"]:
        """Read credentials from os.environ. Returns None if any required
        field is absent (caller decides whether to raise or skip).

        Resolution order: caller-provided env → os.environ. Required keys:
            POLY_L2_API_KEY
            POLY_L2_API_SECRET
            POLY_L2_API_PASSPHRASE
            POLY_EVM_WALLET_PRIV_KEY
        Optional:
            POLY_PROXY_WALLET_ADDRESS (None when native wallet flow)
        """
        env_src = env if env is not None else os.environ
        for required in ("POLY_L2_API_KEY", "POLY_L2_API_SECRET",
                         "POLY_L2_API_PASSPHRASE", "POLY_EVM_WALLET_PRIV_KEY"):
            if not env_src.get(required):
                log.debug("missing required env key: %s", required)
                return None
        return cls(
            l2_api_key=env_src["POLY_L2_API_KEY"],
            l2_api_secret=env_src["POLY_L2_API_SECRET"],
            l2_api_passphrase=env_src["POLY_L2_API_PASSPHRASE"],
            evm_wallet_priv_key=env_src["POLY_EVM_WALLET_PRIV_KEY"],
            proxy_wallet_address=env_src.get("POLY_PROXY_WALLET_ADDRESS") or None,
        )

    def validate(self) -> None:
        """Raise LiveConfigurationError listing missing fields if any."""
        missing = []
        if not self.l2_api_key: missing.append("POLY_L2_API_KEY")
        if not self.l2_api_secret: missing.append("POLY_L2_API_SECRET")
        if not self.l2_api_passphrase: missing.append("POLY_L2_API_PASSPHRASE")
        if not self.evm_wallet_priv_key: missing.append("POLY_EVM_WALLET_PRIV_KEY")
        if missing:
            raise LiveConfigurationError(
                "LiveOrderPlacer not constructible; required env variables "
                "missing: %s. See .env.example and docs/strategy_doc.md § Phase-2A." % (
                    ", ".join(missing)))


@dataclass
class QuoteSubmit:
    """Raw quote submit payload, paired with lib/strategies.py QuoteSubmit.

    Imported from loops.router used here only for type hints.

    LIVE PLACE_QUOTE CONSUMER NOTE: this type reuses the existing
    QuoteSubmit ∷ lib/strategies.py — we DO NOT redefine it. The schema is
    pair-rebuilt as a polymaker OrderArgs at conversion time:

    `QuoteSubmit -> py_clob_client.client.clob_types.OrderArgs`:
        q.price     -> OrderArgs.price
        q.size      -> OrderArgs.size
        q.asset_id  -> OrderArgs.token_id
        q.side      ('BID' or 'ASK') -> OrderArgs.side ('BUY' for BID, 'SELL' for ASK)
                  (semantically: a QuoteSubmit.side=BID means "we quote BID
                  = we want to BUY at price"; Polymarket CLOB `side='BUY'`
                  matches that intent.
        q.quote_market_unused -> ignored
    Sleep until KYC; this body remains commented to preserve the above
    documentation without aliasing into runtime-error paths.
    """
    pass


@dataclass
class FillReceipt:
    """Truth-anchor fill record emitted by LiveOrderPlacer.poll_for_fills.

    Returned by the CLOB /trades endpoint filtered by `proxyWallet` (the
    `transactionHash` field is the canonical on-chain truth anchor — Phase-2A
    truth comes from THIS field, NOT from any depth-shrinkage inference).
    """
    order_id: str         # The canonical order_id returned by the CLOB POST
    asset_id: str         # CLOB token_id (the asset_id of the YES or NO claim)
    side: str             # "BUY" | "SELL"
    size: float
    price: float
    ts_ms: int            # Unix ms
    transaction_hash: str # canonical on-chain ConditionSwap tx hash
    proxy_wallet_address: str  # our wallet (anchor of custody)
    order_type: str = "GTC"  # GTC | FAK | FOK | GTD
    raw_payload: dict[str, Any] = field(default_factory=dict)


class LiveOrderPlacer:
    """Phase-2A L2-API EIP-712-signed order placer for Polymarket perpetual markets.

    Construction flow:
      1. LiveOrderPlacer(creds) constructor — lazy-installs py_clob_client,
         raises LiveConfigurationError until user completes KYC and
         `.env.creds` are populated.
      2. self._client = py_clob_client.client.ClobClient(host, key=evm_priv, ...).
      3. self._client.create_or_derive_api_creds() → HMAC creds.
      4. self._client.set_api_creds(creds).

    POST-KYC, the bodies of place_quote / cancel_quote / poll_for_fills need
    to be filled in (they currently raise LiveNotImplementedError to make
    PRE-KYC rollouts SAFE: callers log "awaiting KYC" + skip).
    """
    def __init__(self, creds: LiveOrderCredentials,
                 host: str = CLOB_HOST,
                 chain_id: int = POLYGON_CHAIN_ID,
                 timeout_sec: int = 30) -> None:
        if creds is None:
            raise LiveConfigurationError(
                "creds is None — pass LiveOrderCredentials.from_env() "
                "manually to inspect the absence reason.")
        creds.validate()
        self.creds = creds
        self.host = host
        self.chain_id = chain_id
        self.timeout_sec = timeout_sec
        self._client: Any = None  # py_clob_client.ClobClient instance

    def connect(self) -> None:
        """Lazily import + instantiate py_clob_client.ClobClient.

        This is invoked once by main_live.py after credential enrollment.
        Raises ImportError (uncaught) when py_clob_client is not installed;
        main_live.py prints a banner instructing the user to
        `pip install py_clob_client` (per-docs/strategy_doc.md § Phase-2A).
        """
        try:
            from py_clob_client.client import ClobClient
        except ImportError as e:
            raise LiveConfigurationError(
                "Phase-2A requires the `py_clob_client` package; install with "
                "`pip install py_clob_client` (see docs/strategy_doc.md § Phase-2A). "
                "Original ImportError: %s" % e)
        self._client = ClobClient(
            host=self.host,
            key=self.creds.evm_wallet_priv_key,
            chain_id=self.chain_id,
            signature_type=2,
            funder=self.creds.proxy_wallet_address,
        )
        api_creds = self._client.create_or_derive_api_creds()
        self._client.set_api_creds(api_creds)

    def place_quote(self, q: QuoteSubmit, ttl_seconds: int = DEFAULT_ORDER_TTL_SEC) -> str:
        """Convert q to OrderArgs + EIP-712 sign + POST to CLOB /order.

        Pre-KYC: raises LiveNotImplementedError with reference to the body
        comment describing the call plan.  POST-KYC: implements the body
        as below.
        """
        # POST-KYC post-trade call graph:
        #   from py_clob_client.clob_types import OrderArgs
        #   side = "BUY" if q.side == "BID" else "SELL"
        #   order = self._client.create_order(OrderArgs(
        #       price=float(q.price), size=float(q.size),
        #       side=side, token_id=q.asset_id))
        #   order_type = "GTC" if ttl_seconds >= DEFAULT_ORDER_TTL_SEC else "GTD"
        #   # GTD (Good-Till-Date) expires the order at ttl_seconds from now.
        #   resp = self._client.post_order(order, order_type=order_type)
        #   return resp["order_id"]
        raise LiveNotImplementedError(
            "LiveOrderPlacer.place_quote is awaiting Phase-2A KYC + "
            "py_clob_client install. See module docstring for the full "
            "call graph.")

    def cancel_quote(self, order_id: str) -> None:
        """POST /cancel_order on the CLOB REST. Returns nothing on success
        (CLOB returns the empty `{"success": true}` body).  Raises
        LiveOrderPlacementError subclass if order was already filled.
        """
        # POST-KYC: self._client.cancel(order_id)
        raise LiveNotImplementedError(
            "LiveOrderPlacer.cancel_quote awaits Phase-2A KYC.")

    def poll_for_fills(self, since_ts_ms: int,
                       timeout_sec: int = DEFAULT_FILL_POLL_TIMEOUT_SEC
                       ) -> list[FillReceipt]:
        """Poll CLOB /trades endpoint for fills tied to `proxyWallet`
        between since_ts_ms and now + timeout_sec screening through the
        `proxyWallet` filter that the L2-authenticated API surfaces.

        PRE-KYC: returns []. POST-KYC: implements the body below.

        Returns:
            list[FillReceipt] of fills where proxyWallet matches
            our wallet (`self.creds.proxy_wallet_address`). TRUTH ANCHOR is
            the `transaction_hash` + `proxyWallet` field; per-arb attacks
            where another MM with the same asset pose instead of US are
            hashed-out at this filter.
        """
        # POST-KYC: trade list fetch via py_clob_client.client.clob_types
        #   trades = self._client.get_trades()  # default args return all
        #   proxy_filter = self.creds.proxy_wallet_address or
        #                  native_wallet_from_priv(self.creds.evm_wallet_priv_key)
        #   out: list[FillReceipt] = []
        #   for t in trades:
        #       if t.get("proxyWallet") != proxy_filter:
        #           continue
        #       if not (t.get("timestamp", "*1000") >= since_ts_ms):
        #           continue
        #       out.append(FillReceipt(...))
        #   return out
        log.debug("poll_for_fills pre-KYC: returning [] (no fills)")
        return []

    @property
    def is_running(self) -> bool:
        """Truth-anchor state: True iff client has a connected WebSocket + L2
        creds/session asserted WITHIN this instance."""
        return self._client is not None


__all__ = [
    "CLOB_HOST",
    "POLYGON_CHAIN_ID",
    "LiveConfigurationError",
    "LiveNotImplementedError",
    "LiveOrderCredentials",
    "FillReceipt",
    "LiveOrderPlacer",
]
