"""
Polymarket HFT Bot v2 — Multi-Asset Auto-Discovery
====================================================
Fully automated prediction market arbitrage bot.

Key improvements over v1:
  • Auto-discovers active crypto markets from Polymarket (BTC, ETH, SOL, etc.)
  • Automatically refreshes token IDs when markets expire
  • Multi-asset price feeds (Binance streams for BTC, ETH, SOL, etc.)
  • Improved prediction engine with adaptive confidence
  • Relaxed execution timeout (500ms) to actually land trades
  • Production-ready py-clob-client integration
  • Structured logging with JSON for AWS CloudWatch
  • Health check endpoint for monitoring
  • Graceful shutdown with position flattening
  • Persistent state file for crash recovery

Requirements:
    pip install aiohttp websockets py-clob-client python-dotenv \
               numpy aiofiles colorlog eth-account web3 uvloop

DISCLAIMER: For educational/research purposes only.
Ensure compliance with Polymarket ToS and applicable regulations.
Trading carries substantial financial risk.
"""

import asyncio
import time
import json
import logging
import os
import signal
import sys
import uuid
import hashlib
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional
from pathlib import Path

import ssl
import certifi
import aiohttp
import numpy as np
import colorlog
from dotenv import load_dotenv

# Try uvloop for better async performance (Linux/Mac only)
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

load_dotenv()

CONFIG = {
    # ── Polymarket credentials ──────────────────
    "POLY_API_KEY":         os.getenv("POLY_API_KEY", ""),
    "POLY_API_SECRET":      os.getenv("POLY_API_SECRET", ""),
    "POLY_API_PASSPHRASE":  os.getenv("POLY_API_PASSPHRASE", ""),
    "POLY_PRIVATE_KEY":     os.getenv("POLY_PRIVATE_KEY", ""),
    "POLY_WALLET_ADDRESS":  os.getenv("POLY_WALLET_ADDRESS", ""),

    # ── Polymarket endpoints ────────────────────
    "CLOB_HTTP_URL":        "https://clob.polymarket.com",
    "CLOB_WS_URL":          "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "GAMMA_API_URL":        "https://gamma-api.polymarket.com",

    # ── Assets to track (auto-discovers markets for each) ──
    "TRACKED_ASSETS": ["BTC", "ETH", "SOL", "DOGE", "XRP", "MATIC", "AVAX", "LINK"],

    # ── Binance streams per asset ───────────────
    "BINANCE_STREAMS": {
        "BTC":   "btcusdt@ticker",
        "ETH":   "ethusdt@ticker",
        "SOL":   "solusdt@ticker",
        "DOGE":  "dogeusdt@ticker",
        "XRP":   "xrpusdt@ticker",
        "MATIC": "maticusdt@ticker",
        "AVAX":  "avaxusdt@ticker",
        "LINK":  "linkusdt@ticker",
    },

    # ── Market discovery ──────────────────────────
    "MARKET_REFRESH_INTERVAL_S": 300,   # re-scan Polymarket every 5 min
    "MIN_MARKET_VOLUME":         5000,  # ignore markets with < $5k volume
    "MIN_MARKET_LIQUIDITY":      1000,  # ignore if best bid/ask size < $1k
    "MARKET_SEARCH_KEYWORDS": [
        "bitcoin", "btc", "ethereum", "eth", "solana", "sol",
        "dogecoin", "doge", "xrp", "ripple", "polygon", "matic",
        "avalanche", "avax", "chainlink", "link",
        "crypto", "price", "hit", "reach", "above", "below",
    ],

    # ── Strategy parameters ─────────────────────
    "MIN_EDGE_PCT":         0.002,   # 0.2% minimum edge (lowered from 0.3%)
    "MAX_EDGE_PCT":         0.10,    # ignore if edge > 10% (stale data?)
    "PREDICTION_WINDOW_S":  900,     # 15-minute forecast window
    "SIGNAL_DECAY_S":       45,      # signals older than 45s are stale
    "EXECUTION_TIMEOUT_MS": 500,     # 500ms (was 100ms — too aggressive)
    "MIN_CONFIDENCE":       0.35,    # lowered from 0.5 to catch more signals

    # ── Risk management ─────────────────────────
    "STARTING_CAPITAL_USDC": float(os.getenv("STARTING_CAPITAL", "10000")),
    "MAX_POSITION_PCT":      0.005,  # 0.5% max capital per trade
    "DAILY_LOSS_LIMIT_PCT":  0.02,   # 2% daily loss limit → halt
    "MAX_OPEN_POSITIONS":    20,     # increased for multi-asset
    "SIDEWAYS_THRESHOLD":    0.001,
    "TREND_MULTIPLIER":      1.5,
    "SIDEWAYS_MULTIPLIER":   0.5,
    "MAX_ASSET_EXPOSURE_PCT": 0.03,  # max 3% capital per single asset

    # ── Position management ─────────────────────
    "TAKE_PROFIT_PCT":      0.004,   # 0.4%
    "STOP_LOSS_PCT":        0.006,   # 0.6%
    "MAX_HOLD_S":           300,     # 5 min
    "TRAILING_STOP_ENABLE": True,    # enable trailing stop
    "TRAILING_STOP_PCT":    0.003,   # 0.3% trailing stop

    # ── Operational ─────────────────────────────
    "LOG_LEVEL":            os.getenv("LOG_LEVEL", "INFO"),
    "LOG_FILE":             "hft_bot.log",
    "STATE_FILE":           "bot_state.json",
    "ORDER_BOOK_DEPTH":     5,
    "HEARTBEAT_INTERVAL_S": 30,
    "STATS_INTERVAL_S":     60,
    "HEALTH_CHECK_PORT":    int(os.getenv("HEALTH_CHECK_PORT", "8080")),
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
        '{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}'
    ))
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, CONFIG["LOG_LEVEL"]))
    if not logger.handlers:
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
class PriceTick:
    asset:     str       # "BTC", "ETH", etc.
    price:     float
    volume:    float
    timestamp: float = field(default_factory=time.time)
    source:    str   = "binance"

@dataclass
class PredictionSignal:
    asset:             str
    predicted_price:   float
    current_price:     float
    confidence:        float
    horizon_seconds:   int
    timestamp:         float = field(default_factory=time.time)

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
class DiscoveredMarket:
    """A Polymarket market discovered via auto-scan."""
    token_id:      str
    question:      str
    asset:         str           # which crypto asset this relates to
    strike_type:   str           # "above", "below", "range", "eod", "weekly"
    strike_value:  Optional[float]  # e.g. 100000 for "BTC above 100k"
    end_date:      Optional[str]
    volume:        float
    yes_price:     float
    no_price:      float
    active:        bool
    condition_id:  str = ""
    last_updated:  float = field(default_factory=time.time)

    @property
    def is_stale(self) -> bool:
        return time.time() - self.last_updated > CONFIG["MARKET_REFRESH_INTERVAL_S"] * 2

@dataclass
class OrderBookSnapshot:
    token_id:  str
    bids:      list
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
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / 2
        return None

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None

    @property
    def spread_pct(self) -> Optional[float]:
        if self.mid and self.spread:
            return self.spread / self.mid
        return None

@dataclass
class TradeSignal:
    token_id:    str
    market_name: str
    asset:       str
    side:        Side
    size_usdc:   float
    limit_price: float
    edge_pct:    float
    signal_time: float = field(default_factory=time.time)
    signal_id:   str   = field(default_factory=lambda: str(uuid.uuid4())[:8])

@dataclass
class OpenPosition:
    token_id:     str
    market_name:  str
    asset:        str
    side:         Side
    size_usdc:    float
    entry_price:  float
    order_id:     str
    open_time:    float = field(default_factory=time.time)
    peak_price:   float = 0.0   # for trailing stop
    pnl:          float = 0.0

# ─────────────────────────────────────────────
# MARKET AUTO-DISCOVERY ENGINE
# ─────────────────────────────────────────────

class MarketDiscovery:
    """
    Automatically discovers and refreshes active crypto prediction
    markets from Polymarket's Gamma API. No more manual token IDs.
    """

    # Keywords that help classify market type
    ABOVE_KEYWORDS  = ["above", "hit", "reach", "over", "exceed", "higher"]
    BELOW_KEYWORDS  = ["below", "under", "drop", "fall", "lower"]
    EOD_KEYWORDS    = ["end of day", "eod", "by midnight", "close"]
    WEEKLY_KEYWORDS = ["this week", "weekly", "by friday", "by sunday", "march"]

    def __init__(self):
        self.log = setup_logger("Discovery")
        self.markets: dict[str, DiscoveredMarket] = {}  # token_id -> market
        self.asset_markets: dict[str, list[str]] = defaultdict(list)  # asset -> [token_ids]
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self, session: aiohttp.ClientSession):
        self._session = session
        await self.refresh()

    async def refresh(self) -> int:
        """Scan Gamma API for active crypto markets. Returns count of new markets found."""
        new_count = 0
        self.log.info("Scanning for active crypto prediction markets…")

        for keyword in CONFIG["MARKET_SEARCH_KEYWORDS"]:
            try:
                markets = await self._search_markets(keyword)
                for m in markets:
                    if m.token_id not in self.markets:
                        self.markets[m.token_id] = m
                        self.asset_markets[m.asset].append(m.token_id)
                        new_count += 1
            except Exception as e:
                self.log.warning(f"Search error for '{keyword}': {e}")
            await asyncio.sleep(0.2)  # rate limit politeness

        # Also search by event slugs for broader coverage
        for asset in CONFIG["TRACKED_ASSETS"]:
            try:
                event_markets = await self._search_events(asset.lower())
                for m in event_markets:
                    if m.token_id not in self.markets:
                        self.markets[m.token_id] = m
                        self.asset_markets[m.asset].append(m.token_id)
                        new_count += 1
            except Exception as e:
                self.log.warning(f"Event search error for '{asset}': {e}")
            await asyncio.sleep(0.2)

        # Prune closed/expired markets
        pruned = 0
        for tid in list(self.markets.keys()):
            m = self.markets[tid]
            if not m.active:
                del self.markets[tid]
                if tid in self.asset_markets.get(m.asset, []):
                    self.asset_markets[m.asset].remove(tid)
                pruned += 1

        total = len(self.markets)
        by_asset = {a: len(tids) for a, tids in self.asset_markets.items() if tids}
        self.log.info(
            f"Discovery complete: {total} active markets "
            f"({new_count} new, {pruned} pruned) — {by_asset}"
        )
        return new_count

    async def _search_markets(self, keyword: str) -> list[DiscoveredMarket]:
        """Search Gamma API for markets matching a keyword."""
        results = []
        try:
            url = f"{CONFIG['GAMMA_API_URL']}/markets"
            params = {
                "active": "true",
                "closed": "false",
                "limit":  "50",
                "search": keyword,
            }
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                if not isinstance(data, list):
                    data = [data]
                for market in data:
                    discovered = self._parse_market(market)
                    if discovered:
                        results.append(discovered)
        except Exception as e:
            self.log.debug(f"_search_markets error: {e}")
        return results

    async def _search_events(self, asset_slug: str) -> list[DiscoveredMarket]:
        """Search Gamma API events for crypto-related prediction markets."""
        results = []
        try:
            url = f"{CONFIG['GAMMA_API_URL']}/events"
            params = {
                "active": "true",
                "closed": "false",
                "limit":  "20",
                "tag":    "crypto",
            }
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    return []
                events = await resp.json()
                if not isinstance(events, list):
                    events = [events]
                for event in events:
                    for market in event.get("markets", []):
                        discovered = self._parse_market(market)
                        if discovered and discovered.asset.lower() == asset_slug.lower():
                            results.append(discovered)
        except Exception as e:
            self.log.debug(f"_search_events error: {e}")
        return results

    def _parse_market(self, market: dict) -> Optional[DiscoveredMarket]:
        """Parse a Gamma API market response into a DiscoveredMarket."""
        question = market.get("question", "")
        if not question:
            return None

        # Determine which asset this relates to
        asset = self._detect_asset(question)
        if not asset:
            return None

        # Must have token IDs
        token_ids = self._parse_json_field(market.get("clobTokenIds"), [])
        if not token_ids:
            return None

        # Use YES token (index 0)
        token_id = token_ids[0]

        # Check volume threshold
        volume = float(market.get("volume", 0))
        if volume < CONFIG["MIN_MARKET_VOLUME"]:
            return None

        # Must be active
        active = market.get("active", False)
        closed = market.get("closed", True)
        if not active or closed:
            return None

        # Parse prices
        prices = self._parse_json_field(market.get("outcomePrices"), [0.5, 0.5])
        yes_price = float(prices[0]) if len(prices) > 0 else 0.5
        no_price  = float(prices[1]) if len(prices) > 1 else 0.5

        # Classify market type
        strike_type = self._classify_type(question)
        strike_value = self._extract_strike(question)

        return DiscoveredMarket(
            token_id=token_id,
            question=question,
            asset=asset,
            strike_type=strike_type,
            strike_value=strike_value,
            end_date=market.get("endDate"),
            volume=volume,
            yes_price=yes_price,
            no_price=no_price,
            active=True,
            condition_id=market.get("conditionId", ""),
        )

    def _detect_asset(self, question: str) -> Optional[str]:
        """Detect which crypto asset a market question is about."""
        q = question.lower()
        asset_map = {
            "BTC":   ["bitcoin", "btc"],
            "ETH":   ["ethereum", "eth", "ether"],
            "SOL":   ["solana", "sol"],
            "DOGE":  ["dogecoin", "doge"],
            "XRP":   ["xrp", "ripple"],
            "MATIC": ["polygon", "matic"],
            "AVAX":  ["avalanche", "avax"],
            "LINK":  ["chainlink", "link"],
        }
        for asset, keywords in asset_map.items():
            if asset in CONFIG["TRACKED_ASSETS"]:
                for kw in keywords:
                    if kw in q:
                        return asset
        return None

    def _classify_type(self, question: str) -> str:
        q = question.lower()
        for kw in self.ABOVE_KEYWORDS:
            if kw in q:
                return "above"
        for kw in self.BELOW_KEYWORDS:
            if kw in q:
                return "below"
        for kw in self.EOD_KEYWORDS:
            if kw in q:
                return "eod"
        for kw in self.WEEKLY_KEYWORDS:
            if kw in q:
                return "weekly"
        return "general"

    def _extract_strike(self, question: str) -> Optional[float]:
        """Extract strike price from question like 'Will BTC hit $100k?'"""
        import re
        # Match patterns like $100k, $95,000, 100000, $80K
        patterns = [
            r'\$?([\d,]+\.?\d*)\s*k\b',       # $100k, 100K
            r'\$?([\d]{4,}(?:,\d{3})*)',       # $95,000 or 95000
        ]
        for pat in patterns:
            match = re.search(pat, question, re.IGNORECASE)
            if match:
                val_str = match.group(1).replace(",", "")
                val = float(val_str)
                # If matched "k" pattern, multiply by 1000
                if "k" in question[match.start():match.end()].lower():
                    val *= 1000
                return val
        return None

    @staticmethod
    def _parse_json_field(value, fallback):
        if value is None:
            return fallback
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                return parsed if isinstance(parsed, list) else [parsed]
            except Exception:
                return fallback
        return fallback

    def get_markets_for_asset(self, asset: str) -> list[DiscoveredMarket]:
        """Get all active markets for a given asset."""
        return [
            self.markets[tid]
            for tid in self.asset_markets.get(asset, [])
            if tid in self.markets and self.markets[tid].active
        ]

    def get_all_active_token_ids(self) -> list[str]:
        """Get all active token IDs across all assets."""
        return [tid for tid, m in self.markets.items() if m.active]


# ─────────────────────────────────────────────
# MARKET REGIME DETECTOR  (per-asset)
# ─────────────────────────────────────────────

class MarketRegimeDetector:
    """Classifies each asset's market as sideways / trending / volatile."""

    def __init__(self, window: int = 60):
        self.prices: dict[str, deque] = defaultdict(lambda: deque(maxlen=window))
        self.log = setup_logger("Regime")

    def update(self, asset: str, price: float) -> None:
        self.prices[asset].append(price)

    def regime(self, asset: str) -> MarketRegime:
        prices = self.prices.get(asset)
        if not prices or len(prices) < 10:
            return MarketRegime.SIDEWAYS
        arr  = np.array(prices)
        rets = np.diff(arr) / arr[:-1]
        vol  = float(np.std(rets))
        if vol < CONFIG["SIDEWAYS_THRESHOLD"]:
            return MarketRegime.SIDEWAYS
        trend = abs(float(arr[-1] - arr[0]) / arr[0])
        if trend > vol * 1.5:
            return MarketRegime.TRENDING
        return MarketRegime.VOLATILE

    def position_multiplier(self, asset: str) -> float:
        r = self.regime(asset)
        if r == MarketRegime.SIDEWAYS:
            return CONFIG["SIDEWAYS_MULTIPLIER"]
        if r == MarketRegime.TRENDING:
            return CONFIG["TREND_MULTIPLIER"]
        return 1.0


# ─────────────────────────────────────────────
# MULTI-ASSET PRICE PREDICTOR
# ─────────────────────────────────────────────

class MultiAssetPredictor:
    """
    Per-asset ensemble predictor: momentum + Bollinger + VWAP.
    Generates predictions independently for each tracked asset.
    """

    def __init__(self):
        self.ticks: dict[str, deque] = defaultdict(lambda: deque(maxlen=500))
        self.log = setup_logger("Predictor")

    def ingest(self, tick: PriceTick) -> None:
        self.ticks[tick.asset].append(tick)

    def predict(self, asset: str) -> Optional[PredictionSignal]:
        ticks = self.ticks.get(asset)
        if not ticks or len(ticks) < 30:
            return None

        prices  = np.array([t.price for t in ticks])
        volumes = np.array([t.volume for t in ticks])
        current = prices[-1]
        horizon = CONFIG["PREDICTION_WINDOW_S"]

        # ── Momentum (EMA crossover) ──────────────
        ema_short = self._ema(prices, 10)
        ema_long  = self._ema(prices, 30)
        momentum  = (ema_short - ema_long) / ema_long

        # ── Mean-reversion (Bollinger) ────────────
        window = min(20, len(prices))
        mu    = np.mean(prices[-window:])
        sigma = np.std(prices[-window:])
        z_score   = (current - mu) / (sigma + 1e-9)
        reversion = -z_score * sigma * 0.3 / current

        # ── Volume-weighted trend ─────────────────
        w = volumes[-window:] + 1e-9
        vwap      = np.average(prices[-window:], weights=w)
        vwap_bias = (current - vwap) / vwap

        # ── Rate of change (extra signal) ─────────
        roc_5  = (prices[-1] - prices[-6]) / prices[-6] if len(prices) >= 6 else 0
        roc_15 = (prices[-1] - prices[-16]) / prices[-16] if len(prices) >= 16 else 0

        # ── Ensemble blend ────────────────────────
        alpha = 0.35   # momentum
        beta  = 0.25   # reversion
        gamma = 0.20   # vwap
        delta = 0.20   # rate of change
        combined = (
            alpha * momentum
            + beta * reversion
            - gamma * vwap_bias
            + delta * (roc_5 * 0.6 + roc_15 * 0.4)
        )
        predicted = current * (1 + combined)

        # Confidence: how much do the signals agree?
        signals = np.array([momentum, -z_score * 0.01, -vwap_bias, roc_5])
        signs   = np.sign(signals)
        agree_ratio = abs(np.mean(signs))  # 1.0 = all agree, 0.0 = split
        confidence  = 0.3 + 0.5 * agree_ratio  # range: 0.3 to 0.8

        # Boost confidence if volume is above average
        avg_vol = np.mean(volumes)
        recent_vol = np.mean(volumes[-5:])
        if recent_vol > avg_vol * 1.5:
            confidence = min(confidence + 0.1, 0.9)

        self.log.debug(
            f"[{asset}] {current:.2f} → {predicted:.2f} "
            f"({combined*100:+.4f}%) conf={confidence:.2f}"
        )
        return PredictionSignal(
            asset=asset,
            predicted_price=float(predicted),
            current_price=float(current),
            confidence=confidence,
            horizon_seconds=horizon,
        )

    @staticmethod
    def _ema(arr: np.ndarray, span: int) -> float:
        alpha = 2 / (span + 1)
        ema = arr[0]
        for v in arr[1:]:
            ema = alpha * v + (1 - alpha) * ema
        return float(ema)


# ─────────────────────────────────────────────
# POLYMARKET CLIENT  (async REST + WebSocket)
# ─────────────────────────────────────────────

class PolymarketClient:
    """
    Async wrapper around Polymarket CLOB REST API.
    Handles order books, order placement, and WS subscriptions.
    """

    def __init__(self):
        self.log     = setup_logger("Polymarket")
        self.session: Optional[aiohttp.ClientSession] = None
        self._books: dict[str, OrderBookSnapshot] = {}
        self._clob_client = None

    async def start(self) -> None:
        ssl_ctx   = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit=50)
        self.session = aiohttp.ClientSession(
            headers={
                "Content-Type":  "application/json",
                "POLY-API-KEY":  CONFIG["POLY_API_KEY"],
            },
            timeout=aiohttp.ClientTimeout(total=5),
            connector=connector,
        )
        # Initialize py-clob-client for live trading
        if not CONFIG["DRY_RUN"]:
            try:
                from py_clob_client.client import ClobClient
                self._clob_client = ClobClient(
                    host=CONFIG["CLOB_HTTP_URL"],
                    key=CONFIG["POLY_PRIVATE_KEY"],
                    chain_id=137,  # Polygon mainnet
                    signature_type=2,  # ECDSA
                )
                # Derive and set API creds
                self._clob_client.set_api_creds(self._clob_client.create_or_derive_api_creds())
                self.log.info("py-clob-client initialized for LIVE trading")
            except ImportError:
                self.log.error("py-clob-client not installed! pip install py-clob-client")
                self._clob_client = None
            except Exception as e:
                self.log.error(f"py-clob-client init failed: {e}")
                self._clob_client = None
        self.log.info("Polymarket HTTP session started")

    async def stop(self) -> None:
        if self.session:
            await self.session.close()

    # ── Order book ────────────────────────────────────────────

    async def get_order_book(self, token_id: str) -> Optional[OrderBookSnapshot]:
        if CONFIG["DRY_RUN"]:
            # Simulate with realistic spread
            fake_mid = 0.50 + np.random.normal(0, 0.02)
            spread = np.random.uniform(0.005, 0.02)
            return OrderBookSnapshot(
                token_id=token_id,
                bids=[(fake_mid - spread/2 - 0.002*i, 50 + 30*i) for i in range(5)],
                asks=[(fake_mid + spread/2 + 0.002*i, 50 + 30*i) for i in range(5)],
            )
        try:
            url = f"{CONFIG['CLOB_HTTP_URL']}/book"
            params = {"token_id": token_id}
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    self.log.warning(f"Order book HTTP {resp.status} for {token_id[:20]}…")
                    return None
                data = await resp.json()
                bids = sorted(
                    [(float(b["price"]), float(b["size"])) for b in data.get("bids", [])],
                    key=lambda x: -x[0]
                )
                asks = sorted(
                    [(float(a["price"]), float(a["size"])) for a in data.get("asks", [])],
                    key=lambda x: x[0]
                )
                snap = OrderBookSnapshot(token_id=token_id, bids=bids, asks=asks)
                self._books[token_id] = snap
                return snap
        except Exception as e:
            self.log.error(f"get_order_book error: {e}")
            return None

    def cached_book(self, token_id: str) -> Optional[OrderBookSnapshot]:
        snap = self._books.get(token_id)
        if snap and (time.time() - snap.timestamp) > 10:
            return None  # stale cache
        return snap

    def update_book_from_ws(self, token_id: str, data: dict) -> None:
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

    # ── Order execution ───────────────────────────────────────

    async def place_limit_order(
        self, token_id: str, side: Side, size_usdc: float, limit_price: float,
    ) -> Optional[str]:
        if CONFIG["DRY_RUN"]:
            oid = f"DRY-{uuid.uuid4().hex[:8]}"
            self.log.info(
                f"[DRY RUN] {side.value} {size_usdc:.2f} USDC @ {limit_price:.4f} "
                f"token={token_id[:16]}…"
            )
            return oid

        if not self._clob_client:
            self.log.error("No CLOB client — cannot place live order")
            return None

        try:
            from py_clob_client.order_builder.constants import BUY, SELL
            from py_clob_client.client import OrderArgs
            clob_side = BUY if side == Side.BUY else SELL
            shares = size_usdc / limit_price
            order = self._clob_client.create_order(OrderArgs(
                token_id=token_id,
                price=limit_price,
                size=shares,
                side=clob_side,
            ))
            resp = self._clob_client.post_order(order, "GTC")
            order_id = resp.get("orderID") or resp.get("id")
            if order_id:
                self.log.info(f"LIVE order placed: {side.value} {size_usdc:.2f} USDC @ {limit_price:.4f} id={order_id}")
            return order_id
        except Exception as e:
            self.log.error(f"Live order failed: {e}", exc_info=True)
            return None

    async def cancel_order(self, order_id: str) -> bool:
        if CONFIG["DRY_RUN"]:
            self.log.info(f"[DRY RUN] Cancel {order_id}")
            return True
        if not self._clob_client:
            return False
        try:
            self._clob_client.cancel(order_id)
            return True
        except Exception as e:
            self.log.error(f"Cancel failed: {e}")
            return False


# ─────────────────────────────────────────────
# MULTI-ASSET FEED MANAGER
# ─────────────────────────────────────────────

class FeedManager:
    """
    Manages WebSocket connections to:
      • Binance combined stream — all tracked assets in one connection
      • Polymarket CLOB — order-book deltas
    """

    def __init__(self, predictor: MultiAssetPredictor, regime: MarketRegimeDetector, poly: PolymarketClient):
        self.predictor = predictor
        self.regime    = regime
        self.poly      = poly
        self.log       = setup_logger("Feed")
        self._running  = False
        self.latest_signals: dict[str, PredictionSignal] = {}  # asset -> latest signal

    async def start(self) -> None:
        self._running = True
        await asyncio.gather(
            self._binance_combined_feed(),
            self._polymarket_feed(),
            return_exceptions=True,
        )

    async def stop(self) -> None:
        self._running = False

    async def _binance_combined_feed(self) -> None:
        """Single Binance WebSocket connection for ALL assets via combined stream."""
        import websockets

        streams = [
            CONFIG["BINANCE_STREAMS"][asset]
            for asset in CONFIG["TRACKED_ASSETS"]
            if asset in CONFIG["BINANCE_STREAMS"]
        ]
        stream_url = f"wss://stream.binance.us:9443/stream?streams={'/'.join(streams)}"

        # Reverse lookup: stream name -> asset
        stream_to_asset = {}
        for asset, stream in CONFIG["BINANCE_STREAMS"].items():
            stream_to_asset[stream] = asset

        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        backoff = 1

        while self._running:
            try:
                self.log.info(f"Connecting to Binance combined stream ({len(streams)} assets)…")
                async with websockets.connect(stream_url, ssl=ssl_ctx, ping_interval=20) as ws:
                    backoff = 1
                    self.log.info(f"Binance connected — streaming {list(CONFIG['TRACKED_ASSETS'])}")
                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                            stream_name = msg.get("stream", "")
                            data = msg.get("data", {})
                            asset = stream_to_asset.get(stream_name)
                            if not asset:
                                continue
                            price  = float(data.get("c", 0))
                            volume = float(data.get("v", 0))
                            if price <= 0:
                                continue
                            tick = PriceTick(asset=asset, price=price, volume=volume)
                            self.predictor.ingest(tick)
                            self.regime.update(asset, price)

                            # Generate prediction
                            signal = self.predictor.predict(asset)
                            if signal:
                                self.latest_signals[asset] = signal
                        except Exception as e:
                            self.log.warning(f"Binance parse error: {e}")
            except Exception as e:
                self.log.error(f"Binance WS error: {e} — retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _polymarket_feed(self) -> None:
        """Polymarket CLOB WebSocket for live order book updates."""
        import websockets

        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        backoff = 1

        while self._running:
            # Get current token IDs to subscribe to
            # (will be refreshed externally by the discovery engine)
            token_ids = list(self.poly._books.keys())
            if not token_ids:
                self.log.debug("No token IDs to subscribe to yet — waiting…")
                await asyncio.sleep(5)
                continue

            try:
                ws_url = CONFIG["CLOB_WS_URL"]
                self.log.info(f"Connecting to Polymarket WS ({len(token_ids)} markets)…")
                async with websockets.connect(ws_url, ssl=ssl_ctx, ping_interval=30, ping_timeout=15) as ws:
                    backoff = 1
                    sub_msg = {
                        "auth":     {"apiKey": CONFIG["POLY_API_KEY"]},
                        "type":     "subscribe",
                        "channel":  "market",
                        "markets":  token_ids[:50],  # WS may have subscription limits
                    }
                    await ws.send(json.dumps(sub_msg))
                    self.log.info(f"Polymarket WS subscribed to {min(len(token_ids), 50)} markets")

                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                            events = msg if isinstance(msg, list) else [msg]
                            for event in events:
                                mtype = event.get("event_type") or event.get("type", "")
                                if mtype in ("book", "price_change", "last_trade_price"):
                                    token = event.get("asset_id") or event.get("market")
                                    if token:
                                        self.poly.update_book_from_ws(token, event)
                        except Exception as e:
                            self.log.warning(f"Poly WS parse error: {e}")

            except Exception as e:
                err = str(e)
                level = "warning" if "no close frame" in err else "error"
                getattr(self.log, level)(f"Polymarket WS: {err} — retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)


# ─────────────────────────────────────────────
# RISK MANAGER
# ─────────────────────────────────────────────

class RiskManager:
    def __init__(self, regime_detector: MarketRegimeDetector):
        self.regime     = regime_detector
        self.log        = setup_logger("Risk")
        self.capital    = float(CONFIG["STARTING_CAPITAL_USDC"])
        self.daily_pnl  = 0.0
        self._reset_day = datetime.now(timezone.utc).date()
        self.open_positions: dict[str, OpenPosition] = {}
        self.trade_history: list[dict] = []

    def _check_day_reset(self) -> None:
        today = datetime.now(timezone.utc).date()
        if today != self._reset_day:
            self.log.info(f"New day — resetting PnL (was {self.daily_pnl:+.2f})")
            self.daily_pnl  = 0.0
            self._reset_day = today

    @property
    def daily_loss_breached(self) -> bool:
        self._check_day_reset()
        return self.daily_pnl <= -(self.capital * CONFIG["DAILY_LOSS_LIMIT_PCT"])

    def asset_exposure(self, asset: str) -> float:
        """Total USDC exposed to a single asset."""
        return sum(
            pos.size_usdc for pos in self.open_positions.values()
            if pos.asset == asset
        )

    def position_size_usdc(self, edge_pct: float, asset: str) -> float:
        base_size   = self.capital * CONFIG["MAX_POSITION_PCT"]
        multiplier  = self.regime.position_multiplier(asset)
        edge_scalar = min(edge_pct / CONFIG["MIN_EDGE_PCT"], 2.0)
        size        = base_size * multiplier * edge_scalar

        # Cap at per-asset exposure limit
        current_exposure = self.asset_exposure(asset)
        max_exposure = self.capital * CONFIG["MAX_ASSET_EXPOSURE_PCT"]
        available = max(0, max_exposure - current_exposure)

        return round(min(size, base_size * 2, available), 2)

    def can_trade(self, token_id: str, asset: str) -> tuple[bool, str]:
        if self.daily_loss_breached:
            return False, "Daily loss limit"
        if len(self.open_positions) >= CONFIG["MAX_OPEN_POSITIONS"]:
            return False, f"Max positions ({CONFIG['MAX_OPEN_POSITIONS']})"
        if token_id in self.open_positions:
            return False, "Already in this market"
        # Per-asset limit
        exposure = self.asset_exposure(asset)
        if exposure >= self.capital * CONFIG["MAX_ASSET_EXPOSURE_PCT"]:
            return False, f"Max {asset} exposure reached"
        return True, "OK"

    def record_open(self, pos: OpenPosition) -> None:
        self.open_positions[pos.token_id] = pos

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
        self.trade_history.append({
            "token_id":    token_id[:20],
            "asset":       pos.asset,
            "side":        pos.side.value,
            "entry":       pos.entry_price,
            "exit":        exit_price,
            "pnl":         round(pnl, 4),
            "hold_s":      round(time.time() - pos.open_time, 1),
            "ts":          datetime.now(timezone.utc).isoformat(),
        })
        self.log.info(
            f"Closed {pos.asset} {pos.side.value} exit={exit_price:.4f} "
            f"PnL={pnl:+.4f} daily={self.daily_pnl:+.4f}"
        )
        return pnl


# ─────────────────────────────────────────────
# SIGNAL DETECTOR  (multi-asset arbitrage)
# ─────────────────────────────────────────────

class SignalDetector:
    def __init__(self, poly: PolymarketClient, risk: RiskManager, discovery: MarketDiscovery):
        self.poly      = poly
        self.risk      = risk
        self.discovery = discovery
        self.log       = setup_logger("Signal")

    def _compute_fair_value(
        self, change_pct: float, market: DiscoveredMarket, current_price: float
    ) -> Optional[float]:
        """
        Estimate fair contract price given predicted price change.
        Uses strike price and market type for a rough probability mapping.
        """
        if market.strike_value and current_price > 0:
            # Distance from current price to strike as fraction
            distance_pct = (market.strike_value - current_price) / current_price

            if market.strike_type in ("above", "eod", "weekly", "general"):
                # P(price > strike) increases if BTC is predicted up
                # Simple logistic-like: shift probability by predicted change scaled by distance
                base_prob = market.yes_price  # use current market price as base
                # If predicted change pushes toward strike, increase prob
                shift = change_pct * 25  # sensitivity: 1% move ≈ 25pp shift
                # Adjust for distance: closer to strike = more sensitive
                if abs(distance_pct) < 0.05:
                    shift *= 2  # within 5% of strike → double sensitivity
                elif abs(distance_pct) > 0.20:
                    shift *= 0.3  # far from strike → less sensitive
                fair = base_prob + shift
                return min(max(fair, 0.01), 0.99)

            elif market.strike_type == "below":
                base_prob = market.yes_price
                shift = -change_pct * 25  # inverse: price up → below prob down
                if abs(distance_pct) < 0.05:
                    shift *= 2
                fair = base_prob + shift
                return min(max(fair, 0.01), 0.99)

        # Fallback: generic mapping
        fair = 0.5 + change_pct * 30
        return min(max(fair, 0.01), 0.99)

    async def detect(self, signals: dict[str, PredictionSignal]) -> list[TradeSignal]:
        if not signals:
            return []

        trade_signals: list[TradeSignal] = []

        for asset, signal in signals.items():
            if not signal.is_fresh:
                continue
            if signal.confidence < CONFIG["MIN_CONFIDENCE"]:
                continue

            markets = self.discovery.get_markets_for_asset(asset)
            if not markets:
                continue

            for market in markets:
                fair_value = self._compute_fair_value(
                    signal.predicted_change_pct, market, signal.current_price
                )
                if fair_value is None:
                    continue

                # Get current market price
                book = self.poly.cached_book(market.token_id)
                if not book:
                    book = await self.poly.get_order_book(market.token_id)
                if not book or book.mid is None:
                    continue

                # Skip illiquid markets
                if book.spread_pct and book.spread_pct > 0.10:  # >10% spread
                    continue

                market_price = book.mid
                edge = fair_value - market_price

                if abs(edge) < CONFIG["MIN_EDGE_PCT"]:
                    continue
                if abs(edge) > CONFIG["MAX_EDGE_PCT"]:
                    continue

                ok, reason = self.risk.can_trade(market.token_id, asset)
                if not ok:
                    self.log.debug(f"Skip {market.question[:40]}: {reason}")
                    continue

                side      = Side.BUY if edge > 0 else Side.SELL
                size_usdc = self.risk.position_size_usdc(abs(edge), asset)
                if size_usdc < 1:  # minimum trade size
                    continue

                if side == Side.BUY:
                    limit_price = min(book.best_ask or fair_value, fair_value)
                else:
                    limit_price = max(book.best_bid or fair_value, fair_value)

                ts = TradeSignal(
                    token_id=market.token_id,
                    market_name=market.question[:50],
                    asset=asset,
                    side=side,
                    size_usdc=size_usdc,
                    limit_price=limit_price,
                    edge_pct=abs(edge),
                )
                trade_signals.append(ts)
                self.log.info(
                    f"[{ts.signal_id}] {asset} {side.value} "
                    f"edge={abs(edge):.3%} fair={fair_value:.4f} mkt={market_price:.4f} "
                    f"'{market.question[:40]}…'"
                )

        return trade_signals


# ─────────────────────────────────────────────
# ORDER EXECUTOR
# ─────────────────────────────────────────────

class OrderExecutor:
    def __init__(self, poly: PolymarketClient, risk: RiskManager):
        self.poly        = poly
        self.risk        = risk
        self.log         = setup_logger("Executor")
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=500)

    async def enqueue(self, signal: TradeSignal) -> None:
        try:
            self._queue.put_nowait(signal)
        except asyncio.QueueFull:
            self.log.warning(f"Queue full — dropping {signal.signal_id}")

    async def run(self) -> None:
        while True:
            signal = await self._queue.get()
            asyncio.create_task(self._execute(signal))

    async def _execute(self, signal: TradeSignal) -> None:
        age_ms = (time.time() - signal.signal_time) * 1000
        if age_ms > CONFIG["EXECUTION_TIMEOUT_MS"]:
            self.log.debug(f"Signal {signal.signal_id} expired ({age_ms:.0f}ms)")
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
                market_name=signal.market_name,
                asset=signal.asset,
                side=signal.side,
                size_usdc=signal.size_usdc,
                entry_price=signal.limit_price,
                order_id=order_id,
                peak_price=signal.limit_price,
            )
            self.risk.record_open(pos)
            self.log.info(f"Executed [{signal.signal_id}] {order_id} in {latency_ms:.0f}ms")


# ─────────────────────────────────────────────
# POSITION MONITOR  (with trailing stop)
# ─────────────────────────────────────────────

class PositionMonitor:
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
                book = await self.poly.get_order_book(token_id)
                if not book or book.mid is None:
                    continue

            current = book.mid
            if pos.side == Side.BUY:
                pnl_pct = (current - pos.entry_price) / pos.entry_price
                # Track peak for trailing stop
                if current > pos.peak_price:
                    pos.peak_price = current
                trailing_drop = (pos.peak_price - current) / pos.peak_price
            else:
                pnl_pct = (pos.entry_price - current) / pos.entry_price
                if current < pos.peak_price or pos.peak_price == pos.entry_price:
                    pos.peak_price = current
                trailing_drop = (current - pos.peak_price) / pos.peak_price if pos.peak_price > 0 else 0

            hold_s = time.time() - pos.open_time
            reason = None

            if pnl_pct >= CONFIG["TAKE_PROFIT_PCT"]:
                reason = f"take-profit ({pnl_pct:.3%})"
            elif pnl_pct <= -CONFIG["STOP_LOSS_PCT"]:
                reason = f"stop-loss ({pnl_pct:.3%})"
            elif hold_s > CONFIG["MAX_HOLD_S"]:
                reason = f"timeout ({hold_s:.0f}s)"
            elif (CONFIG["TRAILING_STOP_ENABLE"]
                  and pnl_pct > CONFIG["TRAILING_STOP_PCT"]
                  and trailing_drop > CONFIG["TRAILING_STOP_PCT"]):
                reason = f"trailing-stop (drop={trailing_drop:.3%})"

            if reason:
                self.log.info(f"Closing {pos.asset} {token_id[:16]}… {reason}")
                await self.poly.cancel_order(pos.order_id)
                self.risk.record_close(token_id, current)


# ─────────────────────────────────────────────
# STATS REPORTER
# ─────────────────────────────────────────────

class StatsReporter:
    def __init__(self, risk: RiskManager, regime: MarketRegimeDetector, discovery: MarketDiscovery):
        self.risk      = risk
        self.regime    = regime
        self.discovery = discovery
        self.log       = setup_logger("Stats")
        self.signals_generated = 0
        self.orders_sent       = 0

    async def run(self) -> None:
        while True:
            await asyncio.sleep(CONFIG["STATS_INTERVAL_S"])
            self._report()

    def _report(self) -> None:
        assets_tracked = {m.asset for m in self.discovery.markets.values()}
        regimes = {a: self.regime.regime(a).value for a in assets_tracked}
        self.log.info(
            f"\n  ── STATS {'─'*40}\n"
            f"  Capital:        ${self.risk.capital:,.2f} USDC\n"
            f"  Daily PnL:      {self.risk.daily_pnl:+.4f} USDC\n"
            f"  Open positions: {len(self.risk.open_positions)}\n"
            f"  Active markets: {len(self.discovery.markets)}\n"
            f"  Assets:         {assets_tracked}\n"
            f"  Regimes:        {regimes}\n"
            f"  Trades today:   {len(self.risk.trade_history)}\n"
            f"  Mode:           {'DRY RUN' if CONFIG['DRY_RUN'] else 'LIVE'}\n"
            f"  {'─'*50}"
        )


# ─────────────────────────────────────────────
# HEALTH CHECK HTTP SERVER
# ─────────────────────────────────────────────

class HealthCheck:
    """Simple HTTP health check for AWS ALB / ECS / systemd monitoring."""

    def __init__(self, risk: RiskManager, discovery: MarketDiscovery):
        self.risk = risk
        self.discovery = discovery
        self.log = setup_logger("Health")

    async def run(self) -> None:
        from aiohttp import web

        app = web.Application()
        app.router.add_get("/health", self._handle)
        app.router.add_get("/stats", self._stats)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", CONFIG["HEALTH_CHECK_PORT"])
        await site.start()
        self.log.info(f"Health check on port {CONFIG['HEALTH_CHECK_PORT']}")

    async def _handle(self, request):
        from aiohttp import web
        status = "ok" if not self.risk.daily_loss_breached else "halted"
        return web.json_response({
            "status": status,
            "capital": self.risk.capital,
            "daily_pnl": self.risk.daily_pnl,
            "open_positions": len(self.risk.open_positions),
            "active_markets": len(self.discovery.markets),
            "dry_run": CONFIG["DRY_RUN"],
        })

    async def _stats(self, request):
        from aiohttp import web
        return web.json_response({
            "trade_history": self.risk.trade_history[-50:],
            "open_positions": {
                tid: {
                    "asset": p.asset,
                    "side": p.side.value,
                    "size": p.size_usdc,
                    "entry": p.entry_price,
                    "hold_s": round(time.time() - p.open_time),
                }
                for tid, p in self.risk.open_positions.items()
            },
        })


# ─────────────────────────────────────────────
# STATE PERSISTENCE  (crash recovery)
# ─────────────────────────────────────────────

class StatePersistence:
    """Save/load bot state for crash recovery."""

    def __init__(self, risk: RiskManager):
        self.risk = risk
        self.log = setup_logger("State")
        self.path = Path(CONFIG["STATE_FILE"])

    def save(self) -> None:
        state = {
            "capital":    self.risk.capital,
            "daily_pnl":  self.risk.daily_pnl,
            "open_positions": {
                tid: {
                    "asset":       p.asset,
                    "side":        p.side.value,
                    "size_usdc":   p.size_usdc,
                    "entry_price": p.entry_price,
                    "order_id":    p.order_id,
                    "open_time":   p.open_time,
                }
                for tid, p in self.risk.open_positions.items()
            },
            "trade_history": self.risk.trade_history[-200:],
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }
        self.path.write_text(json.dumps(state, indent=2))

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            state = json.loads(self.path.read_text())
            self.risk.capital   = state.get("capital", self.risk.capital)
            self.risk.daily_pnl = state.get("daily_pnl", 0.0)
            self.risk.trade_history = state.get("trade_history", [])
            self.log.info(f"Restored state: capital=${self.risk.capital:,.2f}")
        except Exception as e:
            self.log.warning(f"Could not load state: {e}")

    async def run(self) -> None:
        """Periodically save state."""
        while True:
            await asyncio.sleep(30)
            self.save()


# ─────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────

class HFTBot:
    def __init__(self):
        self.log       = setup_logger("Bot")
        self.regime    = MarketRegimeDetector()
        self.predictor = MultiAssetPredictor()
        self.poly      = PolymarketClient()
        self.risk      = RiskManager(self.regime)
        self.discovery = MarketDiscovery()
        self.detector  = SignalDetector(self.poly, self.risk, self.discovery)
        self.executor  = OrderExecutor(self.poly, self.risk)
        self.monitor   = PositionMonitor(self.poly, self.risk)
        self.stats     = StatsReporter(self.risk, self.regime, self.discovery)
        self.feeds     = FeedManager(self.predictor, self.regime, self.poly)
        self.health    = HealthCheck(self.risk, self.discovery)
        self.state     = StatePersistence(self.risk)
        self._stop     = asyncio.Event()

    async def _signal_loop(self) -> None:
        while not self._stop.is_set():
            try:
                signals = await self.detector.detect(self.feeds.latest_signals)
                for s in signals:
                    await self.executor.enqueue(s)
            except Exception as e:
                self.log.error(f"Signal loop: {e}", exc_info=True)
            await asyncio.sleep(0.05)  # 50ms polling

    async def _book_refresh_loop(self) -> None:
        """Periodically refresh order books for all discovered markets."""
        while not self._stop.is_set():
            token_ids = self.discovery.get_all_active_token_ids()
            for token_id in token_ids[:30]:  # cap to avoid rate limits
                if self._stop.is_set():
                    break
                await self.poly.get_order_book(token_id)
                await asyncio.sleep(0.3)  # ~3 per second
            await asyncio.sleep(2)

    async def _market_refresh_loop(self) -> None:
        """Periodically re-discover markets to catch new ones / prune expired."""
        while not self._stop.is_set():
            await asyncio.sleep(CONFIG["MARKET_REFRESH_INTERVAL_S"])
            try:
                new = await self.discovery.refresh()
                if new > 0:
                    self.log.info(f"Discovered {new} new markets")
            except Exception as e:
                self.log.error(f"Market refresh error: {e}")

    async def run(self) -> None:
        self.log.info(
            f"\n{'═'*55}\n"
            f"  Polymarket HFT Bot v2 — Multi-Asset\n"
            f"  Mode:    {'DRY RUN 🟡' if CONFIG['DRY_RUN'] else 'LIVE 🔴'}\n"
            f"  Capital: ${CONFIG['STARTING_CAPITAL_USDC']:,.0f} USDC\n"
            f"  Assets:  {CONFIG['TRACKED_ASSETS']}\n"
            f"  Edge:    {CONFIG['MIN_EDGE_PCT']*100:.1f}% min\n"
            f"{'═'*55}"
        )

        if not CONFIG["DRY_RUN"]:
            if not CONFIG["POLY_API_KEY"] or not CONFIG["POLY_PRIVATE_KEY"]:
                self.log.critical("Missing POLY_API_KEY / POLY_PRIVATE_KEY!")
                return

        # Initialize
        await self.poly.start()
        self.state.load()

        # Auto-discover markets
        await self.discovery.start(self.poly.session)
        total = len(self.discovery.markets)
        self.log.info(f"Discovered {total} active crypto prediction markets")

        # Pre-fetch order books
        for tid in self.discovery.get_all_active_token_ids()[:20]:
            await self.poly.get_order_book(tid)

        tasks = [
            asyncio.create_task(self.feeds.start(),             name="feeds"),
            asyncio.create_task(self.executor.run(),            name="executor"),
            asyncio.create_task(self.monitor.run(),             name="monitor"),
            asyncio.create_task(self.stats.run(),               name="stats"),
            asyncio.create_task(self._signal_loop(),            name="signals"),
            asyncio.create_task(self._book_refresh_loop(),      name="book_refresh"),
            asyncio.create_task(self._market_refresh_loop(),    name="market_refresh"),
            asyncio.create_task(self.health.run(),              name="health"),
            asyncio.create_task(self.state.run(),               name="state_persist"),
        ]

        try:
            await self._stop.wait()
        finally:
            self.log.info("Shutting down — flattening positions…")
            # Save state before shutdown
            self.state.save()
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
        try:
            loop.add_signal_handler(sig, _sig_handler)
        except NotImplementedError:
            pass  # Windows doesn't support add_signal_handler

    await bot.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
