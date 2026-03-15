"""
Bot Diagnostic Script
======================
Run this to find out exactly why your bot isn't trading.
It checks token IDs, order books, API keys, and signal generation.

Usage:
    python3 diagnose.py
"""

import asyncio
import ssl
import json
import os
import certifi
import aiohttp
from dotenv import load_dotenv

load_dotenv()

# ── Load your config ──────────────────────────────────────
API_KEY    = os.getenv("POLY_API_KEY", "")
TOKENS     = {
    "TOKEN_BTC_100K_EOD":  os.getenv("TOKEN_BTC_100K_EOD", ""),
    "TOKEN_BTC_95K_EOD":   os.getenv("TOKEN_BTC_95K_EOD", ""),
    "TOKEN_BTC_WEEKLY_UP": os.getenv("TOKEN_BTC_WEEKLY_UP", ""),
}
CLOB_URL   = "https://clob.polymarket.com"
BINANCE_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
MIN_EDGE   = 0.003  # 0.3%

async def run():
    ssl_ctx   = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=10),
    ) as session:

        print("\n" + "═" * 55)
        print("  BOT DIAGNOSTIC REPORT")
        print("═" * 55)

        # ── 1. Check API key ──────────────────────────────────
        print("\n[1] API KEY")
        if not API_KEY:
            print("  ❌ POLY_API_KEY is empty in your .env file")
        else:
            print(f"  ✅ API key loaded ({API_KEY[:6]}...)")

        # ── 2. Check BTC price from Binance ──────────────────
        print("\n[2] BINANCE BTC PRICE")
        try:
            async with session.get(BINANCE_URL) as resp:
                data  = await resp.json()
                price = float(data["price"])
                print(f"  ✅ Current BTC price: ${price:,.2f}")
        except Exception as e:
            print(f"  ❌ Cannot reach Binance: {e}")
            price = None

        # ── 3. Check each token ID ────────────────────────────
        print("\n[3] TOKEN ID STATUS")
        valid_tokens   = []
        invalid_tokens = []

        for name, token_id in TOKENS.items():
            if not token_id:
                print(f"  ❌ {name}: EMPTY — not set in .env")
                invalid_tokens.append(name)
                continue

            try:
                url = f"{CLOB_URL}/book"
                headers = {"POLY-API-KEY": API_KEY}
                async with session.get(
                    url,
                    params={"token_id": token_id},
                    headers=headers,
                ) as resp:
                    if resp.status == 404:
                        print(f"  ❌ {name}: 404 NOT FOUND — market expired or wrong ID")
                        invalid_tokens.append(name)
                    elif resp.status == 200:
                        book = await resp.json()
                        bids = book.get("bids", [])
                        asks = book.get("asks", [])
                        if bids and asks:
                            best_bid = float(bids[0]["price"])
                            best_ask = float(asks[0]["price"])
                            mid      = (best_bid + best_ask) / 2
                            spread   = best_ask - best_bid
                            print(f"  ✅ {name}: ACTIVE")
                            print(f"       Bid: {best_bid:.4f}  Ask: {best_ask:.4f}  Mid: {mid:.4f}  Spread: {spread:.4f}")
                            valid_tokens.append((name, token_id, mid, best_bid, best_ask))
                        else:
                            print(f"  ⚠️  {name}: Active but NO LIQUIDITY (empty order book)")
                            invalid_tokens.append(name)
                    else:
                        text = await resp.text()
                        print(f"  ❌ {name}: HTTP {resp.status} — {text[:80]}")
                        invalid_tokens.append(name)
            except Exception as e:
                print(f"  ❌ {name}: Error — {e}")
                invalid_tokens.append(name)

        # ── 4. Check signal generation ────────────────────────
        print("\n[4] SIGNAL DETECTION (would the bot trade?)")
        if not valid_tokens:
            print("  ❌ No valid tokens to check — fix token IDs first")
        elif price is None:
            print("  ❌ No BTC price available to generate signals")
        else:
            for name, token_id, mid, best_bid, best_ask in valid_tokens:
                # Rough fair value estimate
                # Bot needs BTC prediction data to be precise,
                # but we can check if the market looks tradeable
                spread = best_ask - best_bid

                if spread > 0.05:
                    print(f"  ⚠️  {name}: Spread too wide ({spread:.4f}) — hard to profit")
                elif mid < 0.01 or mid > 0.99:
                    print(f"  ⚠️  {name}: Contract near resolution ({mid:.4f}) — very low sensitivity")
                else:
                    print(f"  ✅ {name}: Looks tradeable (mid={mid:.4f}, spread={spread:.4f})")

        # ── 5. Summary & fix instructions ────────────────────
        print("\n[5] SUMMARY")
        print(f"  Valid tokens:   {len(valid_tokens)}/3")
        print(f"  Invalid tokens: {len(invalid_tokens)}/3")

        if invalid_tokens:
            print("\n  ⚠️  EXPIRED/INVALID TOKENS — this is why bot isn't trading")
            print("  Fix: Run get_token_ids.py to find fresh active markets")
            print("  Then update these in your .env file:")
            for t in invalid_tokens:
                print(f"    {t}=<new token ID>")

        if not valid_tokens:
            print("\n  ❌ CRITICAL: No valid markets found")
            print("  The bot cannot trade without at least 1 valid token ID")
            print("  Go to polymarket.com and find active BTC markets")
            print("  then run get_token_ids.py to get their token IDs")
        elif len(valid_tokens) == 3:
            print("\n  ✅ All tokens valid — if bot still isn't trading:")
            print("  • Check DRY_RUN=false in your .env")
            print("  • Check signal edges are above 0.3% (MIN_EDGE_PCT)")
            print("  • Market may be in sideways regime reducing trade frequency")

        print("\n" + "═" * 55 + "\n")

if __name__ == "__main__":
    asyncio.run(run())
