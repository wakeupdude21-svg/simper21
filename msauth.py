"""
msauth.py — Microsoft Device Code Flow for Minecraft.

Uses the official Minecraft Launcher client ID with login.live.com.
No Azure subscription or app registration needed.

Split API for Discord embed integration:
  1. request_device_code(session) → get user_code + verification_uri
  2. poll_for_token(session, device_code, interval, expires_in) → MS token
  3. exchange_for_mc_token(session, ms_token) → MC bearer token

Or use authenticate() for the full console-only flow.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

# ──────────────────────────────────────────────────────────────
# Official Minecraft Launcher OAuth2 constants
# NO Azure subscription or app registration is required.
# ──────────────────────────────────────────────────────────────
_CLIENT_ID       = "00000000402b5328"
_SCOPE           = "service::user.auth.xboxlive.com::MBI_SSL"

# login.live.com device-code endpoints
_DEVICE_CODE_URL = "https://login.live.com/oauth20_connect.srf"
_TOKEN_URL       = "https://login.live.com/oauth20_token.srf"

# Xbox / XSTS / Minecraft endpoints
_XBOX_AUTH_URL = "https://user.auth.xboxlive.com/user/authenticate"
_XSTS_AUTH_URL = "https://xsts.auth.xboxlive.com/xsts/authorize"
_MC_AUTH_URL   = "https://api.minecraftservices.com/authentication/login_with_xbox"

# Token cache
_TOKEN_FILE      = Path("token.txt")
_TOKEN_MAX_AGE_S = 23 * 3600


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Token cache — public so the bot can call these directly
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def load_cached_token() -> str | None:
    """Return the cached bearer token if it exists and is < 23 h old."""
    if not _TOKEN_FILE.exists():
        return None
    try:
        data = json.loads(_TOKEN_FILE.read_text(encoding="utf-8"))
        saved_at = data.get("saved_at", 0)
        token    = data.get("token", "")
        if time.time() - saved_at < _TOKEN_MAX_AGE_S and token:
            return token
    except (json.JSONDecodeError, KeyError):
        pass
    return None


def save_token(token: str) -> None:
    """Persist the bearer token with a timestamp."""
    payload = {
        "token": token,
        "saved_at": time.time(),
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _TOKEN_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Split device-code API (for Discord embed flow)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


async def request_device_code(session: aiohttp.ClientSession) -> dict:
    """POST to login.live.com to get a device code.

    Returns dict with keys: user_code, device_code, verification_uri,
    interval, expires_in.
    """
    async with session.post(
        _DEVICE_CODE_URL,
        data={
            "client_id":     _CLIENT_ID,
            "scope":         _SCOPE,
            "response_type": "device_code",
        },
    ) as resp:
        if resp.status != 200:
            body = await resp.text()
            raise RuntimeError(f"[auth] Device code request failed ({resp.status}): {body}")
        data = await resp.json(content_type=None)

    print(f"[auth] Device code: {data.get('user_code')}  ->  {data.get('verification_uri')}")
    return data


async def poll_for_token(
    session: aiohttp.ClientSession,
    device_code: str,
    interval: int = 5,
    expires_in: int = 900,
) -> str:
    """Poll login.live.com until the user completes login.

    Returns the Microsoft access token.
    """
    deadline = time.time() + expires_in

    while time.time() < deadline:
        await asyncio.sleep(interval)

        async with session.post(
            _TOKEN_URL,
            data={
                "client_id":   _CLIENT_ID,
                "device_code": device_code,
                "grant_type":  "urn:ietf:params:oauth:grant-type:device_code",
            },
        ) as resp:
            data = await resp.json(content_type=None)

        error = data.get("error")

        if error == "authorization_pending":
            continue
        elif error == "slow_down":
            interval += 5
            continue
        elif error == "authorization_declined":
            raise RuntimeError("[auth] User declined the login request.")
        elif error == "expired_token":
            raise RuntimeError("[auth] Device code expired -- run !login again.")
        elif error:
            desc = data.get("error_description", "Unknown error")
            raise RuntimeError(f"[auth] Poll error: {error} -- {desc}")

        access_token = data.get("access_token")
        if not access_token:
            raise RuntimeError(f"[auth] No access_token in response: {data}")

        print("[auth] Microsoft login successful!")
        return access_token

    raise RuntimeError("[auth] Device code expired — no login within time limit.")


async def exchange_for_mc_token(session: aiohttp.ClientSession, ms_token: str) -> str:
    """Exchange MS access token -> XBL -> XSTS -> Minecraft bearer token.

    Also saves the token to cache.
    """
    xbl_token, user_hash = await _xbox_live_auth(session, ms_token)
    xsts_token = await _xsts_auth(session, xbl_token)
    mc_token   = await _minecraft_auth(session, xsts_token, user_hash)

    save_token(mc_token)
    print("[auth] Minecraft bearer token acquired and cached.")
    return mc_token


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Full console flow (fallback)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


async def authenticate(account_type: str = "ms", **_kwargs) -> str:
    """Full authentication — prints device code to console and waits."""
    cached = load_cached_token()
    if cached is not None:
        print("[auth] Loaded cached token (< 23 h old) — skipping re-auth.")
        return cached

    account_type = account_type.lower().strip()

    if account_type == "t":
        token = os.environ.get("MC_TOKEN", "").strip()
        if not token:
            token = input("[auth] Paste your Minecraft bearer token: ").strip()
        if not token:
            raise ValueError("No bearer token provided.")
        save_token(token)
        return token

    async with aiohttp.ClientSession() as session:
        code_data = await request_device_code(session)

        user_code = code_data["user_code"]
        uri       = code_data.get("verification_uri", "https://www.microsoft.com/link")
        expires   = code_data.get("expires_in", 900)

        print()
        print("[auth] ═══════════════════════════════════════════════════")
        print(f"[auth]  Go to:  {uri}")
        print(f"[auth]  Enter code:  {user_code}")
        print("[auth] ═══════════════════════════════════════════════════")
        print(f"[auth] Code expires in {expires // 60} minutes.")
        print()

        ms_token = await poll_for_token(
            session,
            code_data["device_code"],
            interval=code_data.get("interval", 5),
            expires_in=expires,
        )
        mc_token = await exchange_for_mc_token(session, ms_token)

    return mc_token


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Internal: Xbox Live → XSTS → Minecraft
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


async def _xbox_live_auth(session: aiohttp.ClientSession, ms_token: str) -> tuple[str, str]:
    payload = {
        "Properties": {
            "AuthMethod": "RPS",
            "SiteName": "user.auth.xboxlive.com",
            "RpsTicket": ms_token,  # login.live.com tokens — no d= prefix
        },
        "RelyingParty": "http://auth.xboxlive.com",
        "TokenType": "JWT",
    }
    async with session.post(_XBOX_AUTH_URL, json=payload) as resp:
        if resp.status != 200:
            body = await resp.text()
            raise RuntimeError(f"[auth] Xbox Live auth failed ({resp.status}): {body}")
        data = await resp.json()
    return data["Token"], data["DisplayClaims"]["xui"][0]["uhs"]


async def _xsts_auth(session: aiohttp.ClientSession, xbl_token: str) -> str:
    payload = {
        "Properties": {
            "SandboxId": "RETAIL",
            "UserTokens": [xbl_token],
        },
        "RelyingParty": "rp://api.minecraftservices.com/",
        "TokenType": "JWT",
    }
    async with session.post(_XSTS_AUTH_URL, json=payload) as resp:
        if resp.status != 200:
            body = await resp.text()
            raise RuntimeError(f"[auth] XSTS auth failed ({resp.status}): {body}")
        data = await resp.json()
    return data["Token"]


async def _minecraft_auth(session: aiohttp.ClientSession, xsts_token: str, user_hash: str) -> str:
    payload = {
        "identityToken": f"XBL3.0 x={user_hash};{xsts_token}",
        "ensureLegacyEnabled": True,
    }
    async with session.post(_MC_AUTH_URL, json=payload) as resp:
        if resp.status != 200:
            body = await resp.text()
            raise RuntimeError(f"[auth] Minecraft auth failed ({resp.status}): {body}")
        data = await resp.json()
    return data["access_token"]
