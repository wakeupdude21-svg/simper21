"""
dropwatcher.py — Continuous mass-scanner for Minecraft username drops.

Scans every valid 3-char (or 4-char) username against the Mojang API
in an infinite loop.  Each request is routed through a rotating proxy
pool (if proxies.txt is present); otherwise falls back to direct
connection with tight throttling.

When a name is detected as AVAILABLE, the on_hit callback fires
immediately — main.py wires this into run_snipe() for auto-claim.

Rate-limit handling:
  • HTTP 429 on a proxy → that proxy is banned for 30 s
  • Connection error on a proxy → banned for 60 s
  • If all proxies are temporarily banned, we wait until one recovers
  • With no proxies, we throttle to ~1 req/sec to stay under limits
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable

import aiohttp

from scrapercheck import CheckResult, NameStatus, is_name_available


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Constants
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789_"
_PROXIES_FILE = Path("proxies.txt")

_BAN_429_S         = 30.0   # proxy cooldown after 429
_BAN_ERROR_S       = 60.0   # proxy cooldown after connection error
_REQ_TIMEOUT_S     = 8.0    # per-request timeout
_DIRECT_THROTTLE_S = 1.0    # delay between checks when no proxies available
_PROXIED_BATCH     = 50     # concurrent checks per batch when using proxies
_DIRECT_BATCH      = 1      # concurrent checks when no proxies (sequential)
_BATCH_GAP_S       = 0.05   # small gap between proxied batches


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Proxy pool
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _normalize_proxy(line: str) -> str:
    """Accept 'host:port', 'user:pass@host:port', or a full URL."""
    line = line.strip()
    if not line or line.startswith("#"):
        return ""
    if "://" in line:
        return line
    # bare "host:port" or "user:pass@host:port" → assume http
    return f"http://{line}"


class ProxyPool:
    """Thread-safe round-robin proxy rotator with per-proxy temp-bans."""

    def __init__(self, path: Path | str = _PROXIES_FILE):
        self._proxies: list[str] = self._load(Path(path))
        self._banned_until: dict[str, float] = {}
        self._idx: int = 0
        self._lock = asyncio.Lock()

    @staticmethod
    def _load(path: Path) -> list[str]:
        if not path.exists():
            return []
        out: list[str] = []
        for raw in path.read_text(encoding="utf-8").splitlines():
            norm = _normalize_proxy(raw)
            if norm:
                out.append(norm)
        return out

    @property
    def size(self) -> int:
        return len(self._proxies)

    @property
    def live_count(self) -> int:
        now = time.time()
        return sum(1 for p in self._proxies if self._banned_until.get(p, 0) < now)

    def reload(self) -> int:
        """Re-read proxies.txt. Returns new pool size."""
        self._proxies = self._load(_PROXIES_FILE)
        self._banned_until.clear()
        self._idx = 0
        return len(self._proxies)

    async def acquire(self) -> str | None:
        """Return the next non-banned proxy, or None if the pool is empty
        or every proxy is currently banned.  Round-robin within live set."""
        async with self._lock:
            if not self._proxies:
                return None
            now = time.time()
            n = len(self._proxies)
            for _ in range(n):
                self._idx = (self._idx + 1) % n
                p = self._proxies[self._idx]
                if self._banned_until.get(p, 0) < now:
                    return p
            return None  # all currently banned

    def ban(self, proxy: str, seconds: float) -> None:
        if proxy:
            self._banned_until[proxy] = time.time() + seconds


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Name generator
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _generate_names(length: int) -> list[str]:
    """All valid Minecraft usernames of given length (shuffled)."""
    if length == 3:
        names = [a + b + c for a in _CHARS for b in _CHARS for c in _CHARS]
    elif length == 4:
        names = [
            a + b + c + d
            for a in _CHARS for b in _CHARS for c in _CHARS for d in _CHARS
        ]
    else:
        raise ValueError("length must be 3 or 4")
    random.shuffle(names)
    return names


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Single-check helper
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


async def _check_one(
    session: aiohttp.ClientSession,
    name: str,
    bearer_token: str | None,
    pool: ProxyPool,
) -> CheckResult | None:
    """Check one name, rotating proxies on failure.  Returns None on
    transient errors so the caller can move on without raising."""
    proxy = await pool.acquire() if pool.size else None
    try:
        result = await asyncio.wait_for(
            is_name_available(session, name, bearer_token, proxy=proxy),
            timeout=_REQ_TIMEOUT_S,
        )
        if result.status == NameStatus.RATE_LIMITED and proxy:
            pool.ban(proxy, _BAN_429_S)
        return result
    except (aiohttp.ClientError, asyncio.TimeoutError):
        if proxy:
            pool.ban(proxy, _BAN_ERROR_S)
        return None
    except Exception:
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DropWatcher
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class DropWatcherStats:
    started_at:      float = 0.0
    checks_total:    int   = 0
    checks_this_min: int   = 0
    hits:            int   = 0
    cycles_done:     int   = 0
    current_cycle:   int   = 0      # index within current pass
    cycle_size:      int   = 0      # total names in current pass
    last_hit_name:   str   = ""
    last_hit_ts:     float = 0.0
    _minute_start:   float = field(default_factory=time.time)


class DropWatcher:
    """Continuous mass-scanner.  Call start() to launch, stop() to halt."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        bearer_token: str | None,
        length: int = 3,
        on_hit: Callable[[CheckResult], Awaitable[None]] | None = None,
    ):
        self._session = session
        self._token = bearer_token
        self._length = length
        self._on_hit = on_hit
        self._pool = ProxyPool()
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self.stats = DropWatcherStats()

    # ── control ──────────────────────────────────────────────

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def proxy_pool(self) -> ProxyPool:
        return self._pool

    def start(self) -> None:
        if self.running:
            return
        self._stop.clear()
        self.stats = DropWatcherStats(started_at=time.time())
        self._task = asyncio.create_task(self._run(), name="dropwatcher")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

    # ── internals ────────────────────────────────────────────

    def _tick_stats(self) -> None:
        self.stats.checks_total    += 1
        self.stats.checks_this_min += 1
        now = time.time()
        if now - self.stats._minute_start >= 60.0:
            # Roll the minute window
            self.stats._minute_start = now
            self.stats.checks_this_min = 1

    async def _run(self) -> None:
        print(
            f"[dropwatch] Starting — length={self._length}, "
            f"proxies={self._pool.size} (live={self._pool.live_count})"
        )
        while not self._stop.is_set():
            names = _generate_names(self._length)
            self.stats.cycle_size = len(names)
            self.stats.current_cycle = 0

            if self._pool.size > 0:
                await self._run_proxied(names)
            else:
                await self._run_direct(names)

            self.stats.cycles_done += 1
            print(
                f"[dropwatch] Cycle {self.stats.cycles_done} complete — "
                f"{self.stats.checks_total} total checks, "
                f"{self.stats.hits} hits"
            )

    async def _run_proxied(self, names: list[str]) -> None:
        """Fire concurrent batches, one proxy per request."""
        for i in range(0, len(names), _PROXIED_BATCH):
            if self._stop.is_set():
                return

            # If no live proxies right now, wait a bit
            if self._pool.live_count == 0:
                print("[dropwatch] All proxies temporarily banned — waiting 5 s")
                await asyncio.sleep(5.0)
                continue

            batch = names[i : i + _PROXIED_BATCH]
            tasks = [
                _check_one(self._session, n, self._token, self._pool)
                for n in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=False)
            for r in results:
                self._tick_stats()
                if r is not None:
                    await self._handle_result(r)

            self.stats.current_cycle += len(batch)
            await asyncio.sleep(_BATCH_GAP_S)

    async def _run_direct(self, names: list[str]) -> None:
        """Sequential checks with throttling — for proxyless mode."""
        for n in names:
            if self._stop.is_set():
                return
            r = await _check_one(self._session, n, self._token, self._pool)
            self._tick_stats()
            self.stats.current_cycle += 1
            if r is not None:
                await self._handle_result(r)
            await asyncio.sleep(_DIRECT_THROTTLE_S)

    async def _handle_result(self, r: CheckResult) -> None:
        if r.status in (NameStatus.AVAILABLE, NameStatus.FREE_404):
            self.stats.hits += 1
            self.stats.last_hit_name = r.name
            self.stats.last_hit_ts = time.time()
            print(f"[dropwatch] HIT — '{r.name}' is AVAILABLE")
            if self._on_hit:
                try:
                    await self._on_hit(r)
                except Exception as exc:
                    print(f"[dropwatch] on_hit error: {exc}")
