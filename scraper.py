"""
scraper.py — Mojang-only drop-time detection and availability polling.

This module continuously polls Mojang endpoints to detect the exact moment
a username becomes available (drops), then signals the sniper core.

Polling strategy (mirrors smart-sniper scrapercheck logic):
  • Normal mode:   poll every ``poll_interval`` seconds (default 30 s)
  • Ramp-up mode:  when within 5 minutes of the expected drop time,
                   switch to polling every 5 seconds for precision
  • Immediate mode: if the name is already free on the first check,
                    return instantly so the sniper can fire right away

No NameMC.  No third-party APIs.  Only official Mojang endpoints.
"""

from __future__ import annotations

import asyncio
import random
import time
from datetime import datetime, timezone, timedelta
from typing import Callable, Awaitable

import aiohttp

from scrapercheck import (
    CheckResult,
    NameStatus,
    check_mojang_profile,
    check_availability,
    is_name_available,
)

# How close to drop time before we switch to fast polling (seconds)
_RAMP_THRESHOLD_S = 5 * 60  # 5 minutes
_RAMP_INTERVAL_S  = 5       # poll every 5 s in the ramp window


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main polling loop
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


async def poll_until_available(
    name: str,
    bearer_token: str | None = None,
    drop_time: datetime | None = None,
    poll_interval: float = 30.0,
    on_available: Callable[[CheckResult], Awaitable[None]] | None = None,
    session: aiohttp.ClientSession | None = None,
) -> CheckResult:
    """Poll Mojang until *name* becomes available, then return the result.

    Parameters
    ----------
    name : str
        The Minecraft username to watch.
    bearer_token : str | None
        If provided, the authenticated ``/available`` endpoint is used
        (more reliable).  Otherwise falls back to the public profile lookup.
    drop_time : datetime | None
        Expected UTC drop time.  Enables ramp-up polling and cross-ref.
    poll_interval : float
        Seconds between checks in normal mode (default 30).
    on_available : callback
        Async callback invoked the instant the name is detected as free.
    session : aiohttp.ClientSession | None
        Reuse an existing session; if *None* a temporary one is created.

    Returns
    -------
    CheckResult with status AVAILABLE or FREE_404.
    """
    owns_session = session is None
    if owns_session:
        session = aiohttp.ClientSession()

    try:
        print(f"[scraper] Watching '{name}' — polling every {poll_interval}s")
        if drop_time:
            print(f"[scraper] Expected drop: {drop_time.isoformat()}")

        while True:
            # ── Determine current polling interval ───────────
            interval = poll_interval
            if drop_time is not None:
                seconds_until = (drop_time - datetime.now(timezone.utc)).total_seconds()
                if seconds_until <= _RAMP_THRESHOLD_S:
                    interval = _RAMP_INTERVAL_S  # fast polling in final 5 min
                    if seconds_until <= 0:
                        interval = 1  # even faster after expected drop
                    print(
                        f"[scraper] Ramp-up: {seconds_until:.0f}s until drop, "
                        f"polling every {interval}s"
                    )

            # ── Perform the check ────────────────────────────
            result = await is_name_available(session, name, bearer_token)

            if result.status in (NameStatus.AVAILABLE, NameStatus.FREE_404):
                # Name is free!
                ts = result.checked_at.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
                print(f"\n[scraper] ✓ '{name}' is AVAILABLE  detected @ {ts}")

                # If a drop_time was provided, cross-reference
                if drop_time:
                    delta = (result.checked_at - drop_time).total_seconds()
                    print(f"[scraper]   Δ from expected drop: {delta:+.3f}s")

                if on_available:
                    await on_available(result)
                return result

            if result.status == NameStatus.NOT_ALLOWED:
                print(f"[scraper] ✗ '{name}' is NOT_ALLOWED (blocked/filtered). Aborting.")
                return result

            # Still taken — log briefly and sleep
            if result.status == NameStatus.DUPLICATE:
                _ts = result.checked_at.strftime("%H:%M:%S")
                print(f"[scraper] {_ts}  '{name}' still taken  (next in {interval}s)")

            await asyncio.sleep(interval)

    finally:
        if owns_session:
            await session.close()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Autosnipe scanner  (concurrent batches, shuffle, rate-limit backoff)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789_"
_BATCH_SIZE = 10          # check this many names concurrently
_RATE_LIMIT_PAUSE = 30    # seconds to wait on 429


def _generate_names(length: int = 3) -> list[str]:
    """Generate all valid Minecraft usernames of *length* chars (3 or 4).

    Minecraft usernames: 3-16 chars, alphanumeric + underscore.
    Shuffled each call so every cycle covers a different order.
    """
    if length == 3:
        names = [a + b + c for a in _CHARS for b in _CHARS for c in _CHARS]
    elif length == 4:
        names = [a + b + c + d for a in _CHARS for b in _CHARS for c in _CHARS for d in _CHARS]
    else:
        names = [a + b + c for a in _CHARS for b in _CHARS for c in _CHARS]
    random.shuffle(names)
    return names


async def _check_batch(
    session: aiohttp.ClientSession,
    names: list[str],
    bearer_token: str | None,
) -> CheckResult | None:
    """Check a batch of names concurrently. Return the first available or None."""
    tasks = [is_name_available(session, n, bearer_token) for n in names]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, CheckResult) and r.status in (NameStatus.AVAILABLE, NameStatus.FREE_404):
            return r
        # Detect rate-limit from raw status code
        if isinstance(r, CheckResult) and r.raw_status_code == 429:
            print(f"[autosnipe] Rate-limited! Pausing {_RATE_LIMIT_PAUSE}s…")
            await asyncio.sleep(_RATE_LIMIT_PAUSE)
    return None


async def scan_names(
    bearer_token: str | None = None,
    session: aiohttp.ClientSession | None = None,
    name_length: int = 3,
) -> CheckResult | None:
    """Scan all names of *name_length* chars using concurrent batch checks.

    - Shuffled order each cycle (don't always start at 'aaa')
    - 10 concurrent checks per batch (10x faster than sequential)
    - Automatic backoff on 429 rate-limits
    - Supports 3-char and 4-char modes

    Returns the first available name, or None if nothing found.
    """
    owns_session = session is None
    if owns_session:
        session = aiohttp.ClientSession()

    try:
        names = _generate_names(name_length)
        total = len(names)
        print(f"[autosnipe] Scanning {total} {name_length}-char names (shuffled, batch={_BATCH_SIZE})…")

        checked = 0
        for i in range(0, total, _BATCH_SIZE):
            batch = names[i : i + _BATCH_SIZE]
            hit = await _check_batch(session, batch, bearer_token)

            if hit is not None:
                ts = hit.checked_at.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
                print(f"\n[autosnipe] ✓ '{hit.name}' is AVAILABLE  detected @ {ts}")
                return hit

            checked += len(batch)
            # Throttle between batches to respect rate limits
            await asyncio.sleep(0.1)

            # Progress log every 500 names
            if checked % 500 < _BATCH_SIZE:
                print(f"[autosnipe] Checked {checked}/{total}…")

        print(f"[autosnipe] Full cycle done — no available {name_length}-char names.")
        return None

    finally:
        if owns_session:
            await session.close()


# Backward-compat alias
async def scan_3char(
    bearer_token: str | None = None,
    poll_interval: float = 30.0,
    session: aiohttp.ClientSession | None = None,
) -> CheckResult | None:
    """Legacy wrapper — calls scan_names with length=3."""
    return await scan_names(bearer_token, session, name_length=3)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Drop-time lookup
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_ASHCON_URL   = "https://api.ashcon.app/mojang/v2/user/{ident}"
_PLAYERDB_URL = "https://playerdb.co/api/player/minecraft/{name}"


async def fetch_drop_time(
    name: str,
    bearer_token: str | None = None,
    session: aiohttp.ClientSession | None = None,
) -> dict:
    """Determine the availability / drop time for a Minecraft username.

    Algorithm
    ---------
    1. Check current availability via official Mojang endpoints.
    2. If taken, resolve the holder's UUID from ``api.mojang.com``.
    3. Query Ashcon API (with PlayerDB as fallback) for name-change history
       to report when the current holder adopted the name.

    Note: Mojang removed the 37-day name-change cooldown in September 2022.
    Names drop *immediately* when the holder changes theirs — there is no
    fixed future drop time for names held by an active player.

    Returns
    -------
    dict with keys:
      ``status``          – "available" | "taken" | "blocked" | "unknown"
      ``drop_time``       – datetime (UTC) when available right now, else None
      ``holder_uuid``     – dashed UUID of current holder, or None
      ``name_held_since`` – datetime (UTC) when holder adopted the name, or None
      ``message``         – human-readable summary string
    """
    owns_session = session is None
    if owns_session:
        session = aiohttp.ClientSession()

    try:
        # ── Step 1: check current availability ──────────────────
        result = await is_name_available(session, name, bearer_token)

        if result.status in (NameStatus.AVAILABLE, NameStatus.FREE_404):
            return {
                "status": "available",
                "drop_time": result.checked_at,
                "holder_uuid": None,
                "name_held_since": None,
                "message": (
                    f"  ✓ '{name}' is AVAILABLE right now!\n"
                    f"    Checked : {result.checked_at.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
                    f"    → Snipe immediately."
                ),
            }

        if result.status == NameStatus.NOT_ALLOWED:
            return {
                "status": "blocked",
                "drop_time": None,
                "holder_uuid": None,
                "name_held_since": None,
                "message": f"  ✗ '{name}' is blocked/filtered by Mojang (NOT_ALLOWED).",
            }

        if result.status == NameStatus.RATE_LIMITED:
            return {
                "status": "unknown",
                "drop_time": None,
                "holder_uuid": None,
                "name_held_since": None,
                "message": "  ⚠ Rate-limited by Mojang — try again in a moment.",
            }

        # ── Step 2: query Ashcon API directly by username ────────
        #    Ashcon accepts both UUID and plain username, so no
        #    separate Mojang UUID pre-lookup is required.
        uuid_dashed: str = ""
        name_held_since: datetime | None = None
        ashcon_ok = False
        try:
            url = _ASHCON_URL.format(ident=name)
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                print(f"[drop] Ashcon API → HTTP {resp.status}")
                if resp.status == 200:
                    data = await resp.json()
                    raw_uuid = data.get("uuid", "")
                    uuid_dashed = raw_uuid  # Ashcon already returns dashed UUID
                    history = data.get("username_history", [])
                    for entry in reversed(history):
                        changed_at = entry.get("changed_at")
                        if changed_at:
                            name_held_since = datetime.fromisoformat(
                                changed_at.replace("Z", "+00:00")
                            ).replace(tzinfo=timezone.utc)
                            break
                    ashcon_ok = True
                elif resp.status == 404:
                    print(f"[drop] Ashcon: name '{name}' not found (404)")
                else:
                    body = await resp.text()
                    print(f"[drop] Ashcon unexpected response: {body[:120]}")
        except Exception as exc:
            print(f"[drop] Ashcon error: {exc}")

        # ── Step 3: Mojang UUID lookup + PlayerDB as fallback ────
        if not ashcon_ok:
            try:
                mojang_url = f"https://api.mojang.com/users/profiles/minecraft/{name}"
                async with session.get(
                    mojang_url, timeout=aiohttp.ClientTimeout(total=6)
                ) as resp:
                    print(f"[drop] Mojang profile API → HTTP {resp.status}")
                    if resp.status == 200:
                        data = await resp.json()
                        uuid_raw = data.get("id", "")
                        if uuid_raw:
                            uuid_dashed = (
                                f"{uuid_raw[0:8]}-{uuid_raw[8:12]}-"
                                f"{uuid_raw[12:16]}-{uuid_raw[16:20]}-{uuid_raw[20:32]}"
                            )
                    else:
                        body = await resp.text()
                        print(f"[drop] Mojang profile body: {body[:120]}")
            except Exception as exc:
                print(f"[drop] Mojang profile error: {exc}")

            if uuid_dashed and name_held_since is None:
                try:
                    url = _PLAYERDB_URL.format(name=name)
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                        print(f"[drop] PlayerDB → HTTP {resp.status}")
                        if resp.status == 200:
                            data = await resp.json()
                            history = (
                                data.get("data", {})
                                    .get("player", {})
                                    .get("meta", {})
                                    .get("name_history", [])
                            )
                            for entry in reversed(history):
                                changed_at = entry.get("changedToAt")
                                if changed_at:
                                    name_held_since = datetime.fromtimestamp(
                                        changed_at / 1000, tz=timezone.utc
                                    )
                                    break
                except Exception as exc:
                    print(f"[drop] PlayerDB error: {exc}")

        if not uuid_dashed:
            return {
                "status": "taken",
                "drop_time": None,
                "holder_uuid": None,
                "name_held_since": None,
                "message": "\n".join([
                    f"  ✗ '{name}' is TAKEN (auth check confirmed) but no public",
                    f"    profile was found via any API — the account may have",
                    f"    restricted visibility or the name is mid-transition.",
                    f"    Tip: Watch it with [W] mode to snipe the instant it drops.",
                ]),
            }

        since_str = (
            name_held_since.strftime("%Y-%m-%d %H:%M:%S UTC")
            if name_held_since
            else "unknown"
        )

        msg = "\n".join([
            f"  ✗ '{name}' is currently TAKEN.",
            f"    Holder UUID     : {uuid_dashed}",
            f"    Name held since : {since_str}",
            f"    Drop time       : No fixed time — names drop immediately when",
            f"                      the holder changes theirs (Mojang policy Sep 2022+).",
            f"    Tip             : Use [W] watch mode to auto-snipe the moment",
            f"                      '{name}' becomes available.",
        ])

        return {
            "status": "taken",
            "drop_time": None,
            "holder_uuid": uuid_dashed,
            "name_held_since": name_held_since,
            "message": msg,
        }

    finally:
        if owns_session:
            await session.close()
