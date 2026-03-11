"""
Polymarket HFT Bot - BTC Prediction Arbitrage
==============================================
Strategy: Exploit latency between external BTC price predictions
and Polymarket contract prices. Detects >0.3% divergence and
executes trades within 100ms.

DISCLAIMER: This is for educational/research purposes only.
Ensure compliance with Polymarket ToS and applicable regulations.
HFT on prediction markets carries substantial financial risk.

Requirements:
    pip install aiohttp websockets py-clob-client python-dotenv
               numpy aiofiles colorlog eth-account web3
"""

import asyncio
import time
import json
import logging
import os
import signal
import sys
import uuid
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional

import ssl
import certifi
import aiohttp
import numpy as np
import colorlog
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

load_dotenv()

CONFIG = {
    # ── Polymarket credentials ──────────────────
    "POLY_API_KEY":         os.getenv("POLY_API_KEY", ""),
    "POLY_API_SECRET":      os.getenv("POLY_API_SECRET", ""),
    "POLY_API_PASSPHRASE":  os.getenv("POLY_API_PASSPHRASE", ""),
    "POLY_PRIVATE_KEY":     os.getenv("POLY_PRIVATE_KEY", ""),   # EVM wallet private key
    "POLY_WALLET_ADDRESS":  os.getenv("POLY_WALLET_ADDRESS", ""),

    # ── Polymarket endpoints ────────────────────
    "CLOB_HTTP_URL":        "https://clob.polymarket.com",
    "CLOB_WS_URL":          "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "GAMMA_API_URL":        "https://gamma-api.polymarket.com",

    # ── BTC Polymarket market token IDs ─────────
    # Fill these in with real token IDs from Polymarket's API
    "BTC_MARKET_TOKENS": {
        "BTC_100K_EOD":   os.getenv("TOKEN_BTC_100K_EOD", ""),
        "BTC_95K_EOD":    os.getenv("TOKEN_BTC_95K_EOD", ""),
        "BTC_WEEKLY_UP":  os.getenv("TOKEN_BTC_WEEKLY_UP", ""),
    },

    # ── External data ───────────────────────────
    "BINANCE_WS_URL":       "wss://stream.binance.com:9443/ws/btcusdt@ticker",
    "COINBASE_WS_URL":      "wss://advanced-trade-ws.coinbase.com",

    # ── Strategy parameters ─────────────────────
    "MIN_EDGE_PCT":         0.003,   # 0.3% minimum edge to trade
    "MAX_EDGE_PCT":         0.08,    # ignore if edge > 8% (stale data?)
    "PREDICTION_WINDOW_S":  900,     # 15-minute BTC forecast window
    "SIGNAL_DECAY_S":       30,      # signals older than this are discarded
    "EXECUTION_TIMEOUT_MS": 100,     # abort trade if >100ms since signal

    # ── Risk management ─────────────────────────
    "STARTING_CAPITAL_USDC": 10_000, # USD equivalent starting capital
    "MAX_POSITION_PCT":      0.005,  # 0.5% max capital per trade
    "DAILY_LOSS_LIMIT_PCT":  0.02,   # 2% daily loss limit → halt trading
    "MAX_OPEN_POSITIONS":    10,     # concurrent open positions
    "SIDEWAYS_THRESHOLD":    0.001,  # BTC 1h vol < 0.1% = sideways market
    "TREND_MULTIPLIER":      1.5,    # scale position up in trending markets
    "SIDEWAYS_MULTIPLIER":   0.5,    # scale position down in sideways

    # ── Operational ─────────────────────────────
    "LOG_LEVEL":            "INFO",
    "LOG_FILE":             "hft_bot.log",
    "ORDER_BOOK_DEPTH":     5,
    "HEARTBEAT_INTERVAL_S": 30,
    "STATS_INTERVAL_S":     60,
    "DRY_RUN":              os.getenv("DRY_RUN", "true").lower() == "true",
}

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

def setup_logger(name: str) -> logging.Logger:
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S.%f",
        log_colors={
            "DEBUG":    "cyan",
            "INFO":     "white",
            "WARNING":  "yellow",
            "ERROR":    "red",
            "CRITICAL": "bold_red",
        }
    ))
    file_handler = logging.FileHandler(CONFIG["LOG_FILE"])
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    ))
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, CONFIG["LOG_LEVEL"]))
    logger.addHandler(handler)
    logger.addHandler(file_handler)
    return logger

log = setup_logger("HFT")

# ─────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────

class Side(Enum):
    BUY  = "BUY"
    SELL = "SELL"

class MarketRegime(Enum):
    SIDEWAYS  = "sideways"
    TRENDING  = "trending"
    VOLATILE  = "volatile"

@dataclass
class BTCTick:
    price:     float
    volume:    float
    timestamp: float = field(default_factory=time.time)
    source:    str   = "unknown"

@dataclass
class PredictionSignal:
    predicted_price:   float
    current_price:     float
    confidence:        float        # 0–1
    horizon_seconds:   int
    timestamp:         float = field(default_factory=time.time)
    source:            str   = "unknown"

    @property
    def predicted_change_pct(self) -> float:
        return (self.predicted_price - self.current_price) / self.current_price

    @property
    def age_seconds(self) -> float:
        return time.time() - self.timestamp

    @property
    def is_fresh(self) -> bool:
        return self.age_seconds < CONFIG["SIGNAL_DECAY_S"]

@dataclass
class OrderBookSnapshot:
    token_id:  str
    bids:      list   # [(price, size), ...]
    asks:      list
    timestamp: float = field(default_factory=time.time)

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0][0] if self.asks else None

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / 2
        return None

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid and self.best_ask:
            return self.best_ask - self.best_bid
        return None

@dataclass
class TradeSignal:
    token_id:    str
    side:        Side
    size_usdc:   float
    limit_price: float
    edge_pct:    float
    signal_time: float = field(default_factory=time.time)
    signal_id:   str   = field(default_factory=lambda: str(uuid.uuid4())[:8])

@dataclass
class OpenPosition:
    token_id:    str
    side:        Side
    size_usdc:   float
    entry_price: float
    order_id:    str
    open_time:   float = field(default_factory=time.time)
    pnl:         float = 0.0

# ─────────────────────────────────────────────
# MARKET REGIME DETECTOR
# ─────────────────────────────────────────────

class MarketRegimeDetector:
    """Classifies BTC market as sideways / trending / volatile."""

    def __init__(self, window: int = 60):
        self.prices: deque = deque(maxlen=window)
        self.log = setup_logger("Regime")

    def update(self, price: float) -> None:
        self.prices.append(price)

    @property
    def regime(self) -> MarketRegime:
        if len(self.prices) < 10:
            return MarketRegime.SIDEWAYS
        arr  = np.array(self.prices)
        rets = np.diff(arr) / arr[:-1]
        vol  = float(np.std(rets))
        if vol < CONFIG["SIDEWAYS_THRESHOLD"]:
            return MarketRegime.SIDEWAYS
        trend = abs(float(arr[-1] - arr[0]) / arr[0])
        if trend > vol * 1.5:
            return MarketRegime.TRENDING
        return MarketRegime.VOLATILE

    def position_multiplier(self) -> float:
        regime = self.regime
        if regime == MarketRegime.SIDEWAYS:
            return CONFIG["SIDEWAYS_MULTIPLIER"]
        if regime == MarketRegime.TRENDING:
            return CONFIG["TREND_MULTIPLIER"]
        return 1.0  # volatile: neutral sizing

# ─────────────────────────────────────────────
# BTC PRICE PREDICTOR  (momentum + mean-reversion ensemble)
# ─────────────────────────────────────────────

class BTCPredictor:
    """
    Lightweight ensemble of simple predictors to generate 15-min
    BTC price forecasts. In production, replace / augment with:
      • CryptoQuant on-chain flows
      • TradingView webhook alerts
      • ML models (LSTM, LightGBM, etc.)
    """

    def __init__(self):
        self.ticks: deque = deque(maxlen=500)
        self.log = setup_logger("Predictor")

    def ingest(self, tick: BTCTick) -> None:
        self.ticks.append(tick)

    def predict(self) -> Optional[PredictionSignal]:
        if len(self.ticks) < 30:
            return None
        prices    = np.array([t.price for t in self.ticks])
        current   = prices[-1]
        horizon   = CONFIG["PREDICTION_WINDOW_S"]

        # ── Momentum (EMA crossover) ──────────────
        ema_short = self._ema(prices, 10)
        ema_long  = self._ema(prices, 30)
        momentum  = (ema_short - ema_long) / ema_long   # signed

        # ── Mean-reversion (Bollinger) ────────────
        mu        = np.mean(prices[-20:])
        sigma     = np.std(prices[-20:])
        z_score   = (current - mu) / (sigma + 1e-9)
        reversion = -z_score * sigma * 0.3              # partial reversion

        # ── Volume-weighted trend ─────────────────
        vols      = np.array([t.volume for t in self.ticks])
        vwap      = np.average(prices[-20:], weights=vols[-20:] + 1e-9)
        vwap_bias = (current - vwap) / vwap

        # ── Ensemble blend ────────────────────────
        alpha          = 0.5   # momentum weight
        beta           = 0.3   # reversion weight
        gamma          = 0.2   # vwap bias weight
        combined_delta = alpha * momentum + beta * (reversion / current) - gamma * vwap_bias
        predicted      = current * (1 + combined_delta)

        # Confidence: higher when signals agree
        signals    = np.array([momentum, -z_score * 0.01, -vwap_bias])
        agree      = np.std(np.sign(signals)) < 0.5
        confidence = 0.7 if agree else 0.4

        self.log.debug(
            f"Prediction: {current:.2f} → {predicted:.2f} "
            f"({combined_delta*100:+.3f}%) conf={confidence:.2f}"
        )
        return PredictionSignal(
            predicted_price=float(predicted),
            current_price=float(current),
            confidence=confidence,
            horizon_seconds=horizon,
            source="ensemble",
        )

    @staticmethod
    def _ema(arr: np.ndarray, span: int) -> float:
        alpha = 2 / (span + 1)
        ema   = arr[0]
        for v in arr[1:]:
            ema = alpha * v + (1 - alpha) * ema
        return ema

# ─────────────────────────────────────────────
# POLYMARKET CLIENT  (async REST + WebSocket)
# ─────────────────────────────────────────────

class PolymarketClient:
    """
    Thin async wrapper around Polymarket CLOB REST API.
    For WebSocket order-book subscriptions and signed order posting.
    """

    def __init__(self):
        self.log     = setup_logger("Polymarket")
        self.session: Optional[aiohttp.ClientSession] = None
        self._books: dict[str, OrderBookSnapshot] = {}

    async def start(self) -> None:
        ssl_ctx   = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_ctx)
        self.session = aiohttp.ClientSession(
            headers={
                "Content-Type":  "application/json",
                "POLY-API-KEY":  CONFIG["POLY_API_KEY"],
            },
            timeout=aiohttp.ClientTimeout(total=5),
            connector=connector,
        )
        self.log.info("Polymarket HTTP session started")

    async def stop(self) -> None:
        if self.session:
            await self.session.close()

    # ── Order book ────────────────────────────────────────────────────────────

    async def get_order_book(self, token_id: str) -> Optional[OrderBookSnapshot]:
        """Fetch L2 order book via REST (fallback when WS lags)."""
        if CONFIG["DRY_RUN"]:
            # Return synthetic book around a fake price for testing
            fake_mid = 0.55
            return OrderBookSnapshot(
                token_id=token_id,
                bids=[(fake_mid - 0.002 * i, 100 * (i+1)) for i in range(5)],
                asks=[(fake_mid + 0.002 * i, 100 * (i+1)) for i in range(5)],
            )
        try:
            url  = f"{CONFIG['CLOB_HTTP_URL']}/book"
            params = {"token_id": token_id}
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    self.log.warning(f"Order book HTTP {resp.status} for {token_id}")
                    return None
                data = await resp.json()
                bids = [(float(b["price"]), float(b["size"])) for b in data.get("bids", [])]
                asks = [(float(a["price"]), float(a["size"])) for a in data.get("asks", [])]
                snap = OrderBookSnapshot(token_id=token_id, bids=bids, asks=asks)
                self._books[token_id] = snap
                return snap
        except Exception as e:
            self.log.error(f"get_order_book error: {e}")
            return None

    def cached_book(self, token_id: str) -> Optional[OrderBookSnapshot]:
        return self._books.get(token_id)

    def update_book_from_ws(self, token_id: str, data: dict) -> None:
        """Update cached order book from WebSocket delta."""
        existing = self._books.get(token_id)
        if not existing:
            return
        for bid in data.get("bids", []):
            p, s = float(bid["price"]), float(bid["size"])
            existing.bids = [(p2, s2) for p2, s2 in existing.bids if p2 != p]
            if s > 0:
                existing.bids.append((p, s))
        existing.bids.sort(key=lambda x: -x[0])
        for ask in data.get("asks", []):
            p, s = float(ask["price"]), float(ask["size"])
            existing.asks = [(p2, s2) for p2, s2 in existing.asks if p2 != p]
            if s > 0:
                existing.asks.append((p, s))
        existing.asks.sort(key=lambda x: x[0])
        existing.timestamp = time.time()

    # ── Order execution ───────────────────────────────────────────────────────

    async def place_limit_order(
        self,
        token_id: str,
        side: Side,
        size_usdc: float,
        limit_price: float,
    ) -> Optional[str]:
        """
        Place a signed limit order on CLOB.
        Returns order_id on success, None on failure.
        """
        if CONFIG["DRY_RUN"]:
            oid = f"DRY-{uuid.uuid4().hex[:8]}"
            self.log.info(
                f"[DRY RUN] {side.value} {size_usdc:.2f} USDC @ {limit_price:.4f} "
                f"token={token_id[:10]}… id={oid}"
            )
            return oid

        # In production, build a properly signed CLOB order.
        # The py-clob-client library handles EIP-712 signing.
        # Example (requires `from py_clob_client.client import ClobClient`):
        #
        # clob = ClobClient(
        #     host=CONFIG["CLOB_HTTP_URL"],
        #     key=CONFIG["POLY_PRIVATE_KEY"],
        #     chain_id=137,  # Polygon
        #     signature_type=2,  # ECDSA
        # )
        # order = clob.create_order(OrderArgs(
        #     token_id=token_id,
        #     price=limit_price,
        #     size=size_usdc / limit_price,  # shares
        #     side=BUY if side == Side.BUY else SELL,
        # ))
        # resp = clob.post_order(order, OrderType.GTC)
        # return resp.get("orderID")

        self.log.error("Live order placement not configured. Set DRY_RUN=false only after full setup.")
        return None

    async def cancel_order(self, order_id: str) -> bool:
        if CONFIG["DRY_RUN"]:
            self.log.info(f"[DRY RUN] Cancel order {order_id}")
            return True
        try:
            url = f"{CONFIG['CLOB_HTTP_URL']}/order/{order_id}"
            async with self.session.delete(url) as resp:
                return resp.status == 200
        except Exception as e:
            self.log.error(f"cancel_order error: {e}")
            return False

# ─────────────────────────────────────────────
# WEBSOCKET FEED MANAGER
# ─────────────────────────────────────────────

class FeedManager:
    """
    Manages WebSocket connections to:
      • Binance  – real-time BTC spot price
      • Coinbase – confirmation feed
      • Polymarket CLOB – order-book deltas
    """

    def __init__(self, predictor: BTCPredictor, regime: MarketRegimeDetector, poly: PolymarketClient):
        self.predictor = predictor
        self.regime    = regime
        self.poly      = poly
        self.log       = setup_logger("Feed")
        self._running  = False
        self.latest_signals: list[PredictionSignal] = []

    async def start(self) -> None:
        self._running = True
        await asyncio.gather(
            self._binance_feed(),
            self._polymarket_feed(),
            return_exceptions=True,
        )

    async def stop(self) -> None:
        self._running = False

    # ── Binance BTC/USDT ticker ───────────────────────────────────────────────

    async def _binance_feed(self) -> None:
        import websockets
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        backoff = 1
        while self._running:
            try:
                self.log.info("Connecting to Binance WebSocket…")
                async with websockets.connect(CONFIG["BINANCE_WS_URL"], ssl=ssl_ctx) as ws:
                    backoff = 1
                    self.log.info("Binance WebSocket connected")
                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                            price  = float(msg["c"])   # last price
                            volume = float(msg["v"])   # 24h volume
                            tick   = BTCTick(price=price, volume=volume, source="binance")
                            self.predictor.ingest(tick)
                            self.regime.update(price)
                            signal = self.predictor.predict()
                            if signal:
                                self.latest_signals.append(signal)
                                if len(self.latest_signals) > 5:
                                    self.latest_signals.pop(0)
                        except Exception as e:
                            self.log.warning(f"Binance msg parse error: {e}")
            except Exception as e:
                self.log.error(f"Binance WS error: {e} — retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    # ── Polymarket CLOB order-book feed ──────────────────────────────────────

    async def _polymarket_feed(self) -> None:
        """
        Polymarket CLOB WebSocket for live order book updates.
        Falls back gracefully — bot still works via REST if this fails.
        """
        import websockets
        ssl_ctx   = ssl.create_default_context(cafile=certifi.where())
        token_ids = [t for t in CONFIG["BTC_MARKET_TOKENS"].values() if t]
        if not token_ids:
            self.log.warning("No Polymarket token IDs configured — skipping WS feed")
            return

        # Polymarket has two WS endpoints depending on what you need
        # Use the subscriptions endpoint with correct asset format
        ws_url  = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        backoff = 1

        while self._running:
            try:
                self.log.info("Connecting to Polymarket CLOB WebSocket…")
                async with websockets.connect(
                    ws_url,
                    ssl=ssl_ctx,
                    ping_interval=30,
                    ping_timeout=15,
                ) as ws:
                    backoff = 1

                    # Correct Polymarket subscription format
                    sub_msg = {
                        "auth":     {"apiKey": CONFIG["POLY_API_KEY"]},
                        "type":     "subscribe",
                        "channel":  "market",
                        "markets":  token_ids,
                    }
                    await ws.send(json.dumps(sub_msg))
                    self.log.info(f"Polymarket WS: subscribed to {len(token_ids)} markets")

                    # Keep reading messages
                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                            # Polymarket sends a list of events or a single event
                            events = msg if isinstance(msg, list) else [msg]
                            for event in events:
                                mtype = event.get("event_type") or event.get("type", "")
                                if mtype in ("book", "price_change", "last_trade_price"):
                                    token = event.get("asset_id") or event.get("market")
                                    if token:
                                        self.poly.update_book_from_ws(token, event)
                        except Exception as e:
                            self.log.warning(f"Poly msg parse error: {e}")

            except Exception as e:
                err = str(e)
                # "no close frame" = server closed connection, not a real error
                # Bot continues fine on REST fallback
                if "no close frame" in err:
                    self.log.warning(
                        f"Polymarket WS disconnected (server closed) — "
                        f"using REST fallback, retry in {backoff}s"
                    )
                else:
                    self.log.error(f"Polymarket WS error: {err} — retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)  # max 2 min between retries

# ─────────────────────────────────────────────
# RISK MANAGER
# ─────────────────────────────────────────────

class RiskManager:
    """
    Enforces position limits, daily loss limits, and
    position sizing based on market regime.
    """

    def __init__(self, regime_detector: MarketRegimeDetector):
        self.regime     = regime_detector
        self.log        = setup_logger("Risk")
        self.capital    = float(CONFIG["STARTING_CAPITAL_USDC"])
        self.daily_pnl  = 0.0
        self._reset_day = datetime.now(timezone.utc).date()
        self.open_positions: dict[str, OpenPosition] = {}

    def _check_day_reset(self) -> None:
        today = datetime.now(timezone.utc).date()
        if today != self._reset_day:
            self.log.info(f"New trading day — resetting daily PnL (was {self.daily_pnl:.2f})")
            self.daily_pnl  = 0.0
            self._reset_day = today

    @property
    def daily_loss_breached(self) -> bool:
        self._check_day_reset()
        limit = self.capital * CONFIG["DAILY_LOSS_LIMIT_PCT"]
        return self.daily_pnl <= -limit

    def position_size_usdc(self, edge_pct: float) -> float:
        """Calculate risk-adjusted position size."""
        base_size   = self.capital * CONFIG["MAX_POSITION_PCT"]
        multiplier  = self.regime.position_multiplier()
        # Scale with edge: higher edge → slightly larger size (Kelly-lite)
        edge_scalar = min(edge_pct / CONFIG["MIN_EDGE_PCT"], 2.0)
        size        = base_size * multiplier * edge_scalar
        return round(min(size, base_size * 2), 2)

    def can_trade(self, token_id: str) -> tuple[bool, str]:
        if self.daily_loss_breached:
            return False, "Daily loss limit reached"
        if len(self.open_positions) >= CONFIG["MAX_OPEN_POSITIONS"]:
            return False, f"Max open positions ({CONFIG['MAX_OPEN_POSITIONS']}) reached"
        if token_id in self.open_positions:
            return False, "Already have open position in this token"
        return True, "OK"

    def record_open(self, pos: OpenPosition) -> None:
        self.open_positions[pos.token_id] = pos
        self.log.info(f"Position opened: {pos.side.value} {pos.size_usdc:.2f} USDC @ {pos.entry_price:.4f}")

    def record_close(self, token_id: str, exit_price: float) -> float:
        pos = self.open_positions.pop(token_id, None)
        if not pos:
            return 0.0
        if pos.side == Side.BUY:
            pnl = pos.size_usdc * (exit_price - pos.entry_price) / pos.entry_price
        else:
            pnl = pos.size_usdc * (pos.entry_price - exit_price) / pos.entry_price
        self.daily_pnl += pnl
        self.capital   += pnl
        self.log.info(
            f"Position closed: {pos.side.value} exit={exit_price:.4f} "
            f"PnL={pnl:+.4f} USDC  daily={self.daily_pnl:+.4f}"
        )
        return pnl

# ─────────────────────────────────────────────
# SIGNAL DETECTOR  (core arbitrage logic)
# ─────────────────────────────────────────────

class SignalDetector:
    """
    Maps BTC price prediction signals to Polymarket contract
    mispricings and generates trade signals.
    """

    def __init__(self, poly: PolymarketClient, risk: RiskManager):
        self.poly = poly
        self.risk = risk
        self.log  = setup_logger("Signal")

    def _btc_change_to_contract_fair(self, change_pct: float, token_name: str) -> Optional[float]:
        """
        Compute the 'fair' Polymarket contract price given a BTC price change.
        This is highly simplified — in production use a proper probability model
        calibrated against historical resolution data.
        """
        # Very rough heuristic: treat contract price as P(BTC > strike at horizon)
        # A positive predicted change increases probability of higher-strike contracts
        if "UP" in token_name or "100K" in token_name:
            # Logistic mapping: a 1% BTC move ≈ 3pp probability shift
            return min(max(0.5 + change_pct * 30, 0.01), 0.99)
        elif "95K" in token_name:
            return min(max(0.6 + change_pct * 20, 0.01), 0.99)
        return None

    async def detect(self, signals: list[PredictionSignal]) -> list[TradeSignal]:
        """Return a list of actionable trade signals."""
        if not signals:
            return []

        # Use the most recent fresh signal
        fresh = [s for s in signals if s.is_fresh and s.confidence > 0.5]
        if not fresh:
            return []
        signal = fresh[-1]

        trade_signals: list[TradeSignal] = []
        for name, token_id in CONFIG["BTC_MARKET_TOKENS"].items():
            if not token_id:
                continue
            fair_value = self._btc_change_to_contract_fair(signal.predicted_change_pct, name)
            if fair_value is None:
                continue

            # Get current market price
            book = self.poly.cached_book(token_id)
            if not book:
                book = await self.poly.get_order_book(token_id)
            if not book or book.mid is None:
                continue

            market_price = book.mid
            edge = fair_value - market_price

            if abs(edge) < CONFIG["MIN_EDGE_PCT"]:
                continue
            if abs(edge) > CONFIG["MAX_EDGE_PCT"]:
                self.log.warning(f"Edge {edge:.3%} too large for {name} — skipping (stale?)")
                continue

            ok, reason = self.risk.can_trade(token_id)
            if not ok:
                self.log.debug(f"Skipping {name}: {reason}")
                continue

            side       = Side.BUY if edge > 0 else Side.SELL
            size_usdc  = self.risk.position_size_usdc(abs(edge))
            # Use limit price slightly aggressive of mid to increase fill probability
            if side == Side.BUY:
                limit_price = min(book.best_ask or fair_value, fair_value)
            else:
                limit_price = max(book.best_bid or fair_value, fair_value)

            ts = TradeSignal(
                token_id=token_id,
                side=side,
                size_usdc=size_usdc,
                limit_price=limit_price,
                edge_pct=abs(edge),
            )
            trade_signals.append(ts)
            self.log.info(
                f"Signal [{ts.signal_id}] {name} {side.value} "
                f"edge={abs(edge):.3%} fair={fair_value:.4f} mkt={market_price:.4f} "
                f"size={size_usdc:.2f} USDC"
            )

        return trade_signals

# ─────────────────────────────────────────────
# ORDER EXECUTOR
# ─────────────────────────────────────────────

class OrderExecutor:
    """
    Executes trade signals with strict latency enforcement.
    Signals older than EXECUTION_TIMEOUT_MS are dropped.
    """

    def __init__(self, poly: PolymarketClient, risk: RiskManager):
        self.poly        = poly
        self.risk        = risk
        self.log         = setup_logger("Executor")
        self._exec_queue: asyncio.Queue = asyncio.Queue(maxsize=200)

    async def enqueue(self, signal: TradeSignal) -> None:
        try:
            self._exec_queue.put_nowait(signal)
        except asyncio.QueueFull:
            self.log.warning(f"Exec queue full — dropping signal {signal.signal_id}")

    async def run(self) -> None:
        """Consume execution queue as fast as possible."""
        while True:
            signal = await self._exec_queue.get()
            asyncio.create_task(self._execute(signal))

    async def _execute(self, signal: TradeSignal) -> None:
        age_ms = (time.time() - signal.signal_time) * 1000
        if age_ms > CONFIG["EXECUTION_TIMEOUT_MS"]:
            self.log.warning(
                f"Signal {signal.signal_id} expired ({age_ms:.1f}ms > "
                f"{CONFIG['EXECUTION_TIMEOUT_MS']}ms) — dropped"
            )
            return

        t0 = time.monotonic()
        order_id = await self.poly.place_limit_order(
            token_id=signal.token_id,
            side=signal.side,
            size_usdc=signal.size_usdc,
            limit_price=signal.limit_price,
        )
        latency_ms = (time.monotonic() - t0) * 1000

        if order_id:
            pos = OpenPosition(
                token_id=signal.token_id,
                side=signal.side,
                size_usdc=signal.size_usdc,
                entry_price=signal.limit_price,
                order_id=order_id,
            )
            self.risk.record_open(pos)
            self.log.info(
                f"Order placed [{signal.signal_id}] id={order_id} "
                f"latency={latency_ms:.1f}ms age={age_ms:.1f}ms"
            )
        else:
            self.log.error(f"Order failed [{signal.signal_id}]")

# ─────────────────────────────────────────────
# POSITION MONITOR  (auto-close logic)
# ─────────────────────────────────────────────

class PositionMonitor:
    """
    Monitors open positions and closes them when:
      • Target profit reached (edge captured)
      • Stop-loss hit
      • Max hold time exceeded
    """

    MAX_HOLD_S   = 300    # 5 minutes max hold
    TAKE_PROFIT  = 0.004  # 0.4% profit target
    STOP_LOSS    = 0.006  # 0.6% stop loss

    def __init__(self, poly: PolymarketClient, risk: RiskManager):
        self.poly = poly
        self.risk = risk
        self.log  = setup_logger("Monitor")

    async def run(self) -> None:
        while True:
            await asyncio.sleep(1)
            await self._check_positions()

    async def _check_positions(self) -> None:
        for token_id, pos in list(self.risk.open_positions.items()):
            book = self.poly.cached_book(token_id)
            if not book or book.mid is None:
                # Fetch fresh
                book = await self.poly.get_order_book(token_id)
                if not book or book.mid is None:
                    continue

            current = book.mid
            if pos.side == Side.BUY:
                pnl_pct = (current - pos.entry_price) / pos.entry_price
            else:
                pnl_pct = (pos.entry_price - current) / pos.entry_price

            hold_s = time.time() - pos.open_time
            reason = None
            if pnl_pct >= self.TAKE_PROFIT:
                reason = f"take-profit ({pnl_pct:.3%})"
            elif pnl_pct <= -self.STOP_LOSS:
                reason = f"stop-loss ({pnl_pct:.3%})"
            elif hold_s > self.MAX_HOLD_S:
                reason = f"max hold time ({hold_s:.0f}s)"

            if reason:
                self.log.info(f"Closing position {token_id[:10]}… reason={reason}")
                await self.poly.cancel_order(pos.order_id)
                self.risk.record_close(token_id, current)

# ─────────────────────────────────────────────
# STATS REPORTER
# ─────────────────────────────────────────────

class StatsReporter:
    def __init__(self, risk: RiskManager, regime: MarketRegimeDetector):
        self.risk   = risk
        self.regime = regime
        self.log    = setup_logger("Stats")
        self.signals_generated = 0
        self.orders_sent       = 0

    async def run(self) -> None:
        while True:
            await asyncio.sleep(CONFIG["STATS_INTERVAL_S"])
            self._report()

    def _report(self) -> None:
        self.log.info(
            f"── STATS ─────────────────────────────────\n"
            f"  Capital:       {self.risk.capital:.2f} USDC\n"
            f"  Daily PnL:     {self.risk.daily_pnl:+.4f} USDC\n"
            f"  Open positions:{len(self.risk.open_positions)}\n"
            f"  Regime:        {self.regime.regime.value}\n"
            f"  Mode:          {'DRY RUN' if CONFIG['DRY_RUN'] else 'LIVE'}\n"
            f"──────────────────────────────────────────"
        )

# ─────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────

class HFTBot:
    """Top-level orchestrator. Wires all components together."""

    def __init__(self):
        self.log      = setup_logger("Bot")
        self.regime   = MarketRegimeDetector()
        self.predictor= BTCPredictor()
        self.poly     = PolymarketClient()
        self.risk     = RiskManager(self.regime)
        self.detector = SignalDetector(self.poly, self.risk)
        self.executor = OrderExecutor(self.poly, self.risk)
        self.monitor  = PositionMonitor(self.poly, self.risk)
        self.stats    = StatsReporter(self.risk, self.regime)
        self.feeds    = FeedManager(self.predictor, self.regime, self.poly)
        self._stop    = asyncio.Event()

    async def _signal_loop(self) -> None:
        """High-frequency signal detection loop."""
        while not self._stop.is_set():
            try:
                signals = await self.detector.detect(self.feeds.latest_signals)
                for s in signals:
                    await self.executor.enqueue(s)
            except Exception as e:
                self.log.error(f"Signal loop error: {e}", exc_info=True)
            await asyncio.sleep(0.01)  # 10ms polling — adjust to taste

    async def _book_refresh_loop(self) -> None:
        """Periodically refresh order books via REST in case WS lags."""
        while not self._stop.is_set():
            for token_id in CONFIG["BTC_MARKET_TOKENS"].values():
                if token_id:
                    await self.poly.get_order_book(token_id)
            await asyncio.sleep(2)

    async def run(self) -> None:
        self.log.info(
            f"═══════════════════════════════════════\n"
            f"  Polymarket HFT Bot starting\n"
            f"  Mode: {'DRY RUN 🟡' if CONFIG['DRY_RUN'] else 'LIVE 🔴'}\n"
            f"  Capital: {CONFIG['STARTING_CAPITAL_USDC']} USDC\n"
            f"  Min edge: {CONFIG['MIN_EDGE_PCT']*100:.2f}%\n"
            f"═══════════════════════════════════════"
        )

        # Validate credentials (non-dry-run)
        if not CONFIG["DRY_RUN"]:
            if not CONFIG["POLY_API_KEY"] or not CONFIG["POLY_PRIVATE_KEY"]:
                self.log.critical("POLY_API_KEY / POLY_PRIVATE_KEY not set! Aborting.")
                return

        await self.poly.start()

        # Fetch initial order books
        for name, tid in CONFIG["BTC_MARKET_TOKENS"].items():
            if tid:
                await self.poly.get_order_book(tid)
                self.log.info(f"Loaded initial order book for {name}")

        tasks = [
            asyncio.create_task(self.feeds.start(),       name="feeds"),
            asyncio.create_task(self.executor.run(),      name="executor"),
            asyncio.create_task(self.monitor.run(),       name="monitor"),
            asyncio.create_task(self.stats.run(),         name="stats"),
            asyncio.create_task(self._signal_loop(),      name="signals"),
            asyncio.create_task(self._book_refresh_loop(),name="book_refresh"),
        ]

        try:
            await self._stop.wait()
        finally:
            self.log.info("Shutting down…")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.poly.stop()
            self.log.info("Shutdown complete.")

    def shutdown(self) -> None:
        self.log.info("Shutdown signal received")
        self._stop.set()

# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

async def main() -> None:
    bot = HFTBot()
    loop = asyncio.get_event_loop()

    def _sig_handler(*_):
        bot.shutdown()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _sig_handler)

    await bot.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


"""
═══════════════════════════════════════════════════════════════
SETUP & DEPLOYMENT GUIDE
═══════════════════════════════════════════════════════════════

1. INSTALL DEPENDENCIES
   pip install aiohttp websockets colorlog numpy python-dotenv eth-account

   Optional (for live trading):
   pip install py-clob-client web3

2. CONFIGURE .env FILE
   Create a .env file alongside this script:

     POLY_API_KEY=your_polymarket_api_key
     POLY_API_SECRET=your_polymarket_api_secret
     POLY_API_PASSPHRASE=your_polymarket_api_passphrase
     POLY_PRIVATE_KEY=0x_your_evm_private_key
     POLY_WALLET_ADDRESS=0x_your_wallet_address

     TOKEN_BTC_100K_EOD=<Polymarket token ID>
     TOKEN_BTC_95K_EOD=<Polymarket token ID>
     TOKEN_BTC_WEEKLY_UP=<Polymarket token ID>

     DRY_RUN=true  # set to false only after thorough testing

3. FIND POLYMARKET TOKEN IDs
   Visit https://gamma-api.polymarket.com/markets?search=bitcoin
   or browse https://polymarket.com and inspect network requests.

4. RUN IN DRY-RUN MODE FIRST
   python polymarket_hft_bot.py

5. PRODUCTION CHECKLIST
   ☐ Token IDs configured and verified
   ☐ API credentials tested
   ☐ Wallet funded with USDC on Polygon
   ☐ USDC approved for CLOB contract
   ☐ Live order placement code uncommented (see place_limit_order)
   ☐ DRY_RUN=false
   ☐ Bot run on low-latency VPS (ideally near Polygon RPC nodes)

PERFORMANCE NOTES
─────────────────
• The 100ms execution target is achievable with co-located infrastructure.
• For 1000+ orders/second you need: Python uvloop, dedicated network,
  and likely C extensions or a Rust/Go side process for order signing.
• Consider: asyncio + uvloop, connection pooling, pre-signed order cache.

LEGAL / RISK REMINDER
──────────────────────
• Review Polymarket Terms of Service before deploying.
• Prediction market trading carries high financial risk.
• This code is for educational purposes. Backtest extensively.
• Never trade capital you cannot afford to lose.
═══════════════════════════════════════════════════════════════
"""
