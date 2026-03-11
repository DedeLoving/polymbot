"""
Polymarket Token ID Fetcher
============================
Fetches CLOB token IDs from Polymarket event URLs.

Requirements:
    pip3 install aiohttp certifi

Usage:
    python3 get_token_ids.py
"""

import asyncio
import ssl
import json
import certifi
import aiohttp

# ─────────────────────────────────────────────
# PASTE YOUR POLYMARKET EVENT URLs HERE
# ─────────────────────────────────────────────
URLS = [
    "https://polymarket.com/event/bitcoin-price-on-march-10",
    "https://polymarket.com/event/what-price-will-bitcoin-hit-march-9-15",
    "https://polymarket.com/event/will-bitcoin-hit-60k-or-80k-first-965",
]

GAMMA_API = "https://gamma-api.polymarket.com"


def extract_slug(url: str) -> str:
    return url.rstrip("/").split("/event/")[-1]


def parse_json_field(value, fallback):
    """
    Polymarket API sometimes returns lists/arrays as raw JSON strings.
    This safely parses them either way.
    """
    if value is None:
        return fallback
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
            return [parsed]
        except Exception:
            return fallback
    return fallback


async def fetch_market(session: aiohttp.ClientSession, url: str) -> None:
    slug = extract_slug(url)
    print(f"\n🔍 Looking up: {slug}")
    print("─" * 60)

    try:
        api_url = f"{GAMMA_API}/events?slug={slug}"
        async with session.get(api_url) as resp:
            if resp.status != 200:
                print(f"  ❌ HTTP {resp.status} — market not found")
                return
            data = await resp.json()

        if not data:
            print("  ❌ No market found for this URL")
            return

        event = data[0] if isinstance(data, list) else data

        print(f"  📌 Event:    {event.get('title', 'N/A')}")
        print(f"  📅 End date: {event.get('endDate', 'N/A')}")
        print(f"  💧 Volume:   ${float(event.get('volume', 0)):,.2f}")
        print(f"  ✅ Active:   {event.get('active', 'N/A')}")
        print(f"  🔒 Closed:   {event.get('closed', 'N/A')}")

        markets = event.get("markets", [])
        if not markets:
            print("  ⚠️  No sub-markets found")
            return

        print(f"\n  Found {len(markets)} market(s):\n")
        for i, market in enumerate(markets, 1):
            question   = market.get("question", "N/A")
            volume     = float(market.get("volume", 0))
            active     = market.get("active", False)
            closed     = market.get("closed", False)
            status     = "✅ ACTIVE" if active and not closed else "❌ CLOSED"

            # Parse prices — API returns these as JSON strings sometimes
            prices     = parse_json_field(market.get("outcomePrices"), ["?", "?"])
            yes_price  = prices[0] if len(prices) > 0 else "?"
            no_price   = prices[1] if len(prices) > 1 else "?"

            # Parse token IDs — same issue
            token_ids  = parse_json_field(market.get("clobTokenIds"), [])

            print(f"  [{i}] {question}")
            print(f"       Status:    {status}")
            print(f"       Volume:    ${volume:,.2f}")
            print(f"       Yes price: {yes_price}  |  No price: {no_price}")

            if token_ids:
                print(f"       Token IDs:")
                labels = ["YES", "NO "]
                for j, tid in enumerate(token_ids):
                    label = labels[j] if j < len(labels) else "   "
                    print(f"         {label} → {tid}")
            else:
                print("       Token IDs: ⚠️  None found")
            print()

    except Exception as e:
        print(f"  ❌ Error: {e}")


async def main():
    print("\n🪙  Polymarket Token ID Fetcher")
    print("=" * 60)

    ssl_ctx   = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    async with aiohttp.ClientSession(
        headers={"Accept": "application/json"},
        timeout=aiohttp.ClientTimeout(total=10),
        connector=connector,
    ) as session:
        for url in URLS:
            await fetch_market(session, url)

    print("=" * 60)
    print("\n📋 Copy the Token IDs into your .env file:")
    print("   TOKEN_BTC_100K_EOD=<YES token ID>")
    print("   TOKEN_BTC_95K_EOD=<YES token ID>")
    print("   TOKEN_BTC_WEEKLY_UP=<YES token ID>")
    print("\n💡 YES token = betting BTC goes UP past that level")
    print("   NO  token = betting BTC stays BELOW that level\n")


if __name__ == "__main__":
    asyncio.run(main())
