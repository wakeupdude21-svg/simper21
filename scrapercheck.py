"""
scrapercheck.py — Availability-status checker for Minecraft usernames.

Uses ONLY official Mojang / Minecraft Services endpoints (no NameMC):

  1. GET https://api.mojang.com/users/profiles/minecraft/<name>
       → 200 = name is taken
       → 404 = name does NOT exist in the system (free right now)

  2. GET https://api.minecraftservices.com/minecraft/profile/name/<name>/available
       (requires bearer token in Authorization header)
       → {"status": "AVAILABLE"}  — claimable right now
       → {"status": "DUPLICATE"}  — currently taken
       → {"status": "NOT_ALLOWED"} — blocked / filtered by Mojang

This module exposes simple async helpers consumed by scraper.py and main.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

import aiohttp


class NameStatus(Enum):
    """Possible states of a Minecraft username."""
    AVAILABLE    = "AVAILABLE"     # can be claimed right now
    DUPLICATE    = "DUPLICATE"     # currently owned by another player
    NOT_ALLOWED  = "NOT_ALLOWED"   # blocked / filtered name
    FREE_404     = "FREE_404"      # Mojang profile lookup returned 404 (name is free)
    RATE_LIMITED = "RATE_LIMITED"  # hit 429 — back off
    UNKNOWN      = "UNKNOWN"       # unexpected API response


@dataclass(frozen=True, slots=True)
class CheckResult:
    """Immutable result from a single availability check."""
    name: str
    status: NameStatus
    checked_at: datetime        # UTC timestamp of the check
    raw_status_code: int        # HTTP status code from the API


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Public async helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


async def check_mojang_profile(
    session: aiohttp.ClientSession,
    name: str,
    proxy: str | None = None,
) -> CheckResult:
    """Check the public Mojang profile endpoint (no auth needed).

    • 200 → name is taken (DUPLICATE)
    • 404 → name is free  (FREE_404)
    """
    url = f"https://api.mojang.com/users/profiles/minecraft/{name}"
    async with session.get(url, proxy=proxy) as resp:
        now = datetime.now(timezone.utc)
        if resp.status == 404:
            return CheckResult(name, NameStatus.FREE_404, now, resp.status)
        if resp.status == 200:
            return CheckResult(name, NameStatus.DUPLICATE, now, resp.status)
        if resp.status == 429:
            return CheckResult(name, NameStatus.RATE_LIMITED, now, resp.status)
        return CheckResult(name, NameStatus.UNKNOWN, now, resp.status)


async def check_availability(
    session: aiohttp.ClientSession,
    name: str,
    bearer_token: str,
    proxy: str | None = None,
) -> CheckResult:
    """Check the authenticated Minecraft Services availability endpoint.

    Requires a valid Minecraft bearer token in the Authorization header.
    Returns AVAILABLE / DUPLICATE / NOT_ALLOWED / UNKNOWN.
    """
    url = f"https://api.minecraftservices.com/minecraft/profile/name/{name}/available"
    headers = {"Authorization": f"Bearer {bearer_token}"}
    async with session.get(url, headers=headers, proxy=proxy) as resp:
        now = datetime.now(timezone.utc)
        if resp.status == 200:
            data = await resp.json()
            status_str = data.get("status", "UNKNOWN").upper()
            try:
                status = NameStatus(status_str)
            except ValueError:
                status = NameStatus.UNKNOWN
            return CheckResult(name, status, now, resp.status)
        if resp.status == 429:
            return CheckResult(name, NameStatus.RATE_LIMITED, now, resp.status)
        return CheckResult(name, NameStatus.UNKNOWN, now, resp.status)


async def is_name_available(
    session: aiohttp.ClientSession,
    name: str,
    bearer_token: str | None = None,
    proxy: str | None = None,
) -> CheckResult:
    """Convenience wrapper: tries the authenticated endpoint first (more
    accurate), falls back to the public Mojang lookup if no token is provided.
    """
    if bearer_token:
        result = await check_availability(session, name, bearer_token, proxy=proxy)
        # If the authenticated check returned a clear answer, use it
        if result.status in (NameStatus.AVAILABLE, NameStatus.DUPLICATE, NameStatus.NOT_ALLOWED):
            return result
    # Fallback — unauthenticated Mojang profile lookup
    return await check_mojang_profile(session, name, proxy=proxy)
