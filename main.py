"""
main.py -- Fully automated MSA-authenticated Minecraft username sniper bot.

Architecture:
  * Authenticates via msauth.py ONCE at startup (token cached 23 h -- no re-auth)
  * Discord bot is the sole interface -- zero interactive prompts after launch
  * Single shared aiohttp.ClientSession created on on_ready, reused for all ops
  * All commands respond with rich Discord embeds

Speed optimisations:
  1. Single persistent TCPConnector session -- reuses TCP connections
  2. DNS pre-resolution at startup -- no per-request DNS lookup
  3. Busy-wait spin-loop for sub-ms fire-time accuracy
  4. N concurrent async workers all firing simultaneously
  5. time.perf_counter_ns() -- nanosecond clock, zero jitter
  6. TCP warm-up HEAD request 10 s before scheduled drop
  7. Zero I/O / zero allocations inside the hot PUT path
  8. Auto delay tuning after every timed snipe

Commands:
  !snipe <name> [drop_time_iso] [delay_ms] [workers]
  !autosnipe <name> [delay_ms] [workers]
  !superfastsnipe <name>
  !scrapetime <name>
  !dropwatch start [3|4] | stop | status
  !cancel <name>
  !status
  !setdelay [ms]
  !help
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
load_dotenv()  # load .env before anything reads os.environ

import aiohttp
from aiohttp import TCPConnector

import msauth
import scraper
from dropwatcher import DropWatcher
from scrapercheck import NameStatus
import os
from threading import Thread
from flask import Flask
# Import your bot library (e.g., discord, hikari, or your custom async loop)

# 1. Create a tiny Flask app
app = Flask('')

@app.route('/')
def home():
    return "Bot is alive!"

def run_web_server():
    # Render requires binding to 0.0.0.0 and the assigned PORT variable
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# 2. Start the web server in a background thread
def keep_alive():
    t = Thread(target=run_web_server)
    t.start()

# 3. Main execution block
if __name__ == "__main__":
    # Start the web server first so Render sees the open port immediately
    keep_alive()
    
    # 4. NOW start your bot or sniper script below
    print("Starting bot loop...")
    # example: client.run(os.environ.get('TOKEN'))

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Constants
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_MC_NAME_CHANGE_URL = "https://api.minecraftservices.com/minecraft/profile/name/{name}"
_MC_SERVICES_HOST   = "api.minecraftservices.com"
_CONFIG_FILE        = Path("config.json")
_SNIPED_LOG         = Path("sniped.txt")
_SUPER_WORKERS      = 10   # fixed worker count for !superfastsnipe

# Embed accent colours
_CLR_GREEN  = 0x2ECC71
_CLR_RED    = 0xE74C3C
_CLR_BLUE   = 0x3498DB
_CLR_GOLD   = 0xF1C40F
_CLR_PURPLE = 0x9B59B6
_CLR_GREY   = 0x95A5A6


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Config
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _load_config() -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "delay": 0,
        "workers": 5,
        "poll_interval": 30,
        "account_type": "ms",
    }
    if _CONFIG_FILE.exists():
        try:
            with open(_CONFIG_FILE, encoding="utf-8") as fh:
                defaults.update(json.load(fh))
        except (json.JSONDecodeError, OSError) as exc:
            print(f"[config] Warning: {exc}")
    # Secrets come from .env / environment variables -- never from config.json
    defaults["discord_token"] = os.environ.get("DISCORD_BOT_TOKEN", "")
    defaults["webhook_url"]   = os.environ.get("DISCORD_WEBHOOK_URL", "")
    return defaults


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Low-level helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _resolve_host(host: str) -> str:
    """Resolve DNS once at startup to skip per-request lookups."""
    try:
        ip = socket.gethostbyname(host)
        print(f"[dns] {host} -> {ip}")
        return ip
    except socket.gaierror:
        print(f"[dns] Resolution failed for {host} -- using hostname directly")
        return host


def _busy_wait_until_ns(target_ns: int) -> None:
    """Coarse sleep + spin-loop for sub-ms fire-time accuracy.

    Phase 1 -- asyncio.sleep has ~1–15 ms jitter, so we sleep until
               200 ms before target to save CPU.
    Phase 2 -- tight spin-loop; burns one core but guarantees <0.1 ms
               accuracy at the actual fire moment.
    """
    sleep_ns = target_ns - time.perf_counter_ns() - 200_000_000
    if sleep_ns > 0:
        time.sleep(sleep_ns / 1_000_000_000)
    while time.perf_counter_ns() < target_ns:
        pass


async def _warmup(session: aiohttp.ClientSession, name: str) -> None:
    """HEAD request to complete the TCP+TLS handshake before the fire."""
    url = _MC_NAME_CHANGE_URL.format(name=name)
    try:
        async with session.head(url) as resp:
            print(f"[warmup] HEAD → {resp.status}")
    except Exception as exc:
        print(f"[warmup] {exc}")


def _log_snipe(name: str, summary: dict) -> None:
    """Append a successful snipe to sniped.txt."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = (
        f"[{ts}]  {name}  |  latency={summary.get('latency_min', 0):.2f}ms"
        f"  worker={summary.get('winner_worker', '?')}\n"
    )
    with open(_SNIPED_LOG, "a", encoding="utf-8") as fh:
        fh.write(line)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Snipe worker (hot path -- zero I/O, zero alloc except the PUT)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


async def _snipe_worker(
    worker_id: int,
    session: aiohttp.ClientSession,
    name: str,
    bearer_token: str,
    fire_ns: int,
    results: list[dict],
    success_event: asyncio.Event,
) -> None:
    url = _MC_NAME_CHANGE_URL.format(name=name)
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }

    _busy_wait_until_ns(fire_ns)

    # -- HOT PATH ---------------------------------------------
    t0 = time.perf_counter_ns()
    try:
        async with session.put(url, headers=headers) as resp:
            t1 = time.perf_counter_ns()
            status = resp.status
            body   = await resp.text()
    except Exception as exc:
        t1     = time.perf_counter_ns()
        status = -1
        body   = str(exc)
    # -- END HOT PATH -----------------------------------------

    latency_ms    = (t1 - t0) / 1_000_000
    fire_time_utc = datetime.now(timezone.utc)

    results.append({
        "worker_id":    worker_id,
        "status":       status,
        "latency_ms":   latency_ms,
        "fire_time_utc": fire_time_utc,
        "body":         body,
    })
    print(f"  [w{worker_id}] {status}  {latency_ms:.2f}ms")

    if status == 200 and not success_event.is_set():
        success_event.set()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Snipe orchestrator
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


async def run_snipe(
    name: str,
    bearer_token: str,
    drop_time: datetime | None = None,
    delay_ms: float = 0.0,
    workers: int = 5,
    session: aiohttp.ClientSession | None = None,
) -> dict[str, Any]:
    """Orchestrate *workers* concurrent PUT attempts to claim *name*.

    Returns a summary dict with latency stats, success flag, and
    auto-tuned delay recommendation for the next attempt.
    """
    owns_session = session is None
    if owns_session:
        connector = TCPConnector(limit=workers, ttl_dns_cache=0, enable_cleanup_closed=True)
        session = aiohttp.ClientSession(connector=connector)

    try:
        # Warm up connection pool 10 s before the drop
        if drop_time is not None:
            seconds_until = (drop_time - datetime.now(timezone.utc)).total_seconds()
            lead = max(seconds_until - 10.0, 0.0)
            if lead > 0:
                print(f"[snipe] Sleeping {lead:.1f}s → then warming up...")
                await asyncio.sleep(lead)
            await _warmup(session, name)

        # Compute the absolute fire timestamp in perf_counter_ns
        if drop_time is not None:
            delta_s = (drop_time - datetime.now(timezone.utc)).total_seconds()
            fire_ns = (
                time.perf_counter_ns()
                + int(delta_s * 1_000_000_000)
                - int(delay_ms * 1_000_000)   # subtract delay so we fire early
            )
        else:
            fire_ns = time.perf_counter_ns()   # immediate

        print(f"[snipe] Firing {workers} workers for '{name}'...")

        results:       list[dict]  = []
        success_event: asyncio.Event = asyncio.Event()

        tasks = [
            asyncio.create_task(
                _snipe_worker(i, session, name, bearer_token, fire_ns, results, success_event)
            )
            for i in range(workers)
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

        latencies = [r["latency_ms"] for r in results if r["status"] != -1]
        success   = any(r["status"] == 200 for r in results)
        winner    = next((r for r in results if r["status"] == 200), None)

        summary: dict[str, Any] = {
            "name":          name,
            "success":       success,
            "attempts":      len(results),
            "latency_min":   min(latencies)              if latencies else 0.0,
            "latency_mean":  statistics.mean(latencies)  if latencies else 0.0,
            "latency_max":   max(latencies)              if latencies else 0.0,
            "winner_worker": winner["worker_id"]         if winner else None,
            "input_delay_ms": delay_ms,
            "tuned_delay_ms": delay_ms,
            "results":       results,
        }

        # Auto-tune: measure how far off the actual fire time was
        if drop_time is not None and results:
            actual_offset_ms = (results[0]["fire_time_utc"] - drop_time).total_seconds() * 1000
            summary["actual_offset_ms"] = actual_offset_ms
            summary["tuned_delay_ms"]   = delay_ms + actual_offset_ms
            print(f"[snipe] offset={actual_offset_ms:+.1f}ms  tuned_delay={summary['tuned_delay_ms']:.1f}ms")

        if success:
            print(f"[snipe] [OK] Claimed '{name}' -- latency {summary['latency_min']:.2f}ms")
            _log_snipe(name, summary)
            print("\a", end="", flush=True)
        else:
            print(f"[snipe] [X] Missed '{name}'")

        return summary

    finally:
        if owns_session:
            await session.close()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Discord (requires discord.py)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_discord_available = False
try:
    import discord
    from discord.ext import commands
    _discord_available = True
except ImportError:
    pass


# -- Embed builders -------------------------------------------

def _embed_snipe_result(name: str, summary: dict[str, Any]) -> "discord.Embed":
    success = summary["success"]
    outcome = "Claimed" if success else "Missed"
    embed = discord.Embed(
        title=f"{outcome}  --  {name}",
        color=_CLR_GREEN if success else _CLR_RED,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(
        name="Latency  (min / avg / max)",
        value=(
            f"`{summary.get('latency_min', 0):.2f}` / "
            f"`{summary.get('latency_mean', 0):.2f}` / "
            f"`{summary.get('latency_max', 0):.2f}` ms"
        ),
        inline=False,
    )
    embed.add_field(name="Workers",    value=str(summary.get("attempts", 0)),                inline=True)
    embed.add_field(name="Winner",     value=f"Worker {summary.get('winner_worker', '--')}",  inline=True)
    embed.add_field(name="Delay",      value=f"{summary.get('input_delay_ms', 0):.1f} ms",   inline=True)
    if summary.get("tuned_delay_ms") != summary.get("input_delay_ms"):
        embed.add_field(
            name="Tuned Delay",
            value=f"{summary['tuned_delay_ms']:.1f} ms",
            inline=True,
        )
    if "actual_offset_ms" in summary:
        embed.add_field(
            name="Fire Offset",
            value=f"{summary['actual_offset_ms']:+.2f} ms",
            inline=True,
        )
    embed.set_footer(text="Repins Sniper  *  MSA Authenticated")
    return embed


def _embed_scrapetime(name: str, info: dict) -> "discord.Embed":
    status = info["status"]
    colour_map = {
        "available": _CLR_GREEN,
        "blocked":   _CLR_RED,
        "taken":     _CLR_GOLD,
    }
    colour = colour_map.get(status, _CLR_GREY)

    embed = discord.Embed(
        title=f"{name}  .  {status.upper()}",
        color=colour,
        timestamp=datetime.now(timezone.utc),
    )

    if info.get("holder_uuid"):
        embed.add_field(name="UUID", value=f"`{info['holder_uuid']}`", inline=False)

    if info.get("name_held_since"):
        since: datetime = info["name_held_since"]
        since_str = since.strftime("%Y-%m-%d  %H:%M:%S UTC")
        delta = datetime.now(timezone.utc) - since
        d, rem = divmod(int(delta.total_seconds()), 86400)
        h, _   = divmod(rem, 3600)
        embed.add_field(name="Held Since", value=since_str,    inline=True)
        embed.add_field(name="Duration",   value=f"{d}d {h}h", inline=True)

    if status == "taken":
        embed.add_field(
            name="Drop Policy",
            value=(
                "Mojang removed the 37-day cooldown in Sep 2022.\n"
                "Names drop **instantly** when the holder changes theirs.\n"
                "Use `!autosnipe` to monitor and fire the instant it drops."
            ),
            inline=False,
        )
    elif status == "available":
        embed.add_field(
            name="Suggested Action",
            value=f"`!snipe {name}`  or  `!superfastsnipe {name}`",
            inline=False,
        )
    elif status == "blocked":
        embed.add_field(
            name="Note",
            value="This name is filtered by Mojang and cannot be claimed.",
            inline=False,
        )

    embed.set_footer(text="Repins Sniper  *  Mojang / Ashcon / PlayerDB")
    return embed


def _embed_help() -> "discord.Embed":
    embed = discord.Embed(
        title="Repins Sniper  --  Command Reference",
        description=(
            "High-performance Minecraft username sniper. "
            "Authenticated via **Microsoft (MSA)** at startup -- token cached 23 h."
        ),
        color=_CLR_BLUE,
        timestamp=datetime.now(timezone.utc),
    )
    cmds = [
        ("`!snipe <name> [drop_time] [delay_ms] [workers]`",
         "Claim a name immediately, or schedule for an ISO 8601 UTC drop time.\n"
         "Example: `!snipe Notch 2025-06-01T12:00:00 -50 8`"),

        ("`!autosnipe <name> [delay_ms] [workers]`",
         "Check current status, then poll every 5 s until the name drops and fire automatically."),

        (f"`!superfastsnipe <name>`",
         f"{_SUPER_WORKERS} workers, 0 ms delay, TCP pre-warmed. Use when the name is already available."),

        ("`!scrapetime <name>`",
         "Check availability, holder UUID, and duration held. Queries Mojang, Ashcon, and PlayerDB."),

        ("`!dropwatch start [3|4]` . `stop` . `status`",
         "Continuously scan every 3 or 4-char name against Mojang using a rotating proxy pool. "
         "Auto-fires a superfast snipe the instant any name drops."),

        ("`!cancel <name>`",
         "Stop a running autosnipe job for the given username."),

        ("`!status`",
         "List all active jobs and their current state."),

        ("`!setdelay [ms]`",
         "Override the auto-tuned fire delay. Omit the value to read the current setting."),

        ("`!help`",
         "Show this reference."),
    ]
    for name_field, value_field in cmds:
        embed.add_field(name=name_field, value=value_field, inline=False)

    embed.set_footer(text="Repins Sniper  *  MSA Authenticated  *  Persistent session")
    return embed


def _embed_status(active_jobs: dict) -> "discord.Embed":
    embed = discord.Embed(
        title="Active Jobs",
        color=_CLR_BLUE,
        timestamp=datetime.now(timezone.utc),
    )
    if not active_jobs:
        embed.description = "No active jobs running."
    else:
        for job_name, task in active_jobs.items():
            state = "Running" if not task.done() else "Done"
            embed.add_field(name=job_name, value=state, inline=True)
    embed.set_footer(text="Repins Sniper")
    return embed


def _embed_simple(msg: str, color: int) -> "discord.Embed":
    """One-line helper for quick informational embeds."""
    return discord.Embed(description=msg, color=color, timestamp=datetime.now(timezone.utc))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Bot factory
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _build_bot(cfg: dict, bearer_ref: list[str]) -> Any:
    """Build the Discord bot.  *bearer_ref* is a mutable single-element list
    so all closures always see the latest token without rebinding.

    Returns the bot instance, or None if discord.py / token is missing.
    """
    if not _discord_available:
        print("[bot] discord.py not installed -- bot disabled")
        return None
    token = cfg.get("discord_token", "")
    if not token:
        print("[bot] No discord_token -- bot disabled")
        return None

    intents = discord.Intents.default()
    intents.message_content = True
    bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

    active_jobs:    dict[str, asyncio.Task] = {}
    tuned_delay:    float = float(cfg.get("delay", 0))
    _sess_holder:   list[aiohttp.ClientSession] = []   # populated in on_ready
    _watcher_holder: list[DropWatcher] = []            # populated by !dropwatch start

    def _tok() -> str:
        return bearer_ref[0]

    def _sess() -> aiohttp.ClientSession | None:
        return _sess_holder[0] if _sess_holder else None

    # -- Webhook ----------------------------------------------

    async def _post_webhook(embed: "discord.Embed") -> None:
        url = cfg.get("webhook_url", "")
        if not url:
            return
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(url, json={"embeds": [embed.to_dict()]}) as r:
                    if r.status >= 400:
                        print(f"[webhook] {r.status}")
        except Exception as exc:
            print(f"[webhook] {exc}")

    # -- Events -----------------------------------------------

    # -- Device-code auth helper (posts Discord embed) --------

    async def _run_device_code_auth(channel: "discord.TextChannel") -> None:
        """Request a device code, post the login embed, poll until done."""
        sess = _sess()
        if sess is None:
            await channel.send(embed=_embed_simple("Session not ready -- try again.", _CLR_RED))
            return

        try:
            code_data = await msauth.request_device_code(sess)
        except Exception as exc:
            await channel.send(embed=_embed_simple(f"Device code request failed: `{exc}`", _CLR_RED))
            return

        user_code = code_data["user_code"]
        uri       = code_data.get("verification_uri", "https://www.microsoft.com/link")
        expires   = code_data.get("expires_in", 900)

        login_embed = discord.Embed(
            title="\U0001f510 Microsoft Login Required",
            color=_CLR_GOLD,
            timestamp=datetime.now(timezone.utc),
        )
        login_embed.add_field(
            name="",
            value=(
                f"1. Open: [{uri}]({uri})\n"
                f"2. Enter code:  `{user_code}`\n"
                f"3. Sign in with your Microsoft account"
            ),
            inline=False,
        )
        login_embed.add_field(
            name="",
            value=f"\u23f3 Code expires in **{expires // 60}** minutes",
            inline=False,
        )
        login_embed.set_footer(text="Repins Sniper  *  Device Code Login")
        await channel.send(embed=login_embed)

        # Poll in background until the user logs in
        try:
            ms_token = await msauth.poll_for_token(
                sess,
                code_data["device_code"],
                interval=code_data.get("interval", 5),
                expires_in=expires,
            )
            mc_token = await msauth.exchange_for_mc_token(sess, ms_token)
            bearer_ref[0] = mc_token

            ok_embed = discord.Embed(
                title="\u2705 Authentication Successful",
                description=f"Token cached for 23 hours.\nToken: `\u2026{mc_token[-8:]}`",
                color=_CLR_GREEN,
                timestamp=datetime.now(timezone.utc),
            )
            ok_embed.set_footer(text="Repins Sniper")
            await channel.send(embed=ok_embed)
            print(f"[auth] Authenticated via Discord -- token ...{mc_token[-8:]}")
        except Exception as exc:
            fail_embed = discord.Embed(
                title="\u274c Authentication Failed",
                description=f"`{exc}`\nRun `!login` to try again.",
                color=_CLR_RED,
                timestamp=datetime.now(timezone.utc),
            )
            fail_embed.set_footer(text="Repins Sniper")
            await channel.send(embed=fail_embed)

    @bot.event
    async def on_ready():
        if _sess_holder:
            return  # on_ready can fire multiple times -- only init once
        max_conn = max(int(cfg.get("workers", 5)), _SUPER_WORKERS) + 4
        connector = TCPConnector(limit=max_conn, ttl_dns_cache=86400, enable_cleanup_closed=True)
        session = aiohttp.ClientSession(connector=connector)
        _sess_holder.append(session)
        bot._shared_session = session  # type: ignore[attr-defined]

        tok = _tok()
        if tok:
            print(f"[bot] Online as {bot.user}  |  session ready  |  token ...{tok[-8:]}")
        else:
            print(f"[bot] Online as {bot.user}  |  session ready  |  NO TOKEN -- waiting for !login or auto-auth")

        await bot.change_presence(
            activity=discord.Activity(type=discord.ActivityType.watching, name="usernames drop")
        )

        # Auto-trigger device code auth if no cached token
        if not tok:
            channel = None
            for guild in bot.guilds:
                for ch in guild.text_channels:
                    if ch.permissions_for(guild.me).send_messages:
                        channel = ch
                        break
                if channel:
                    break
            if channel:
                asyncio.create_task(_run_device_code_auth(channel))
            else:
                print("[auth] No text channel found -- use !login in any channel")

    @bot.event
    async def on_command_error(ctx, error):
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(embed=_embed_simple(
                f"Missing argument -- run `!help` for usage.", _CLR_RED
            ))
        elif isinstance(error, commands.CommandNotFound):
            pass
        else:
            await ctx.send(embed=_embed_simple(f"`{error}`", _CLR_RED))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Commands
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @bot.command(name="help")
    async def cmd_help(ctx):
        await ctx.send(embed=_embed_help())

    # -- !login -----------------------------------------------

    @bot.command(name="login")
    async def cmd_login(ctx):
        """Trigger Microsoft Device Code login via Discord embed."""
        asyncio.create_task(_run_device_code_auth(ctx.channel))

    # -- !snipe -----------------------------------------------

    @bot.command(name="snipe")
    async def cmd_snipe(
        ctx,
        username:      str,
        drop_time_iso: str = "",
        delay_ms_str:  str = "",
        workers_str:   str = "",
    ):
        nonlocal tuned_delay
        drop_time: datetime | None = None
        if drop_time_iso:
            try:
                drop_time = datetime.fromisoformat(drop_time_iso).replace(tzinfo=timezone.utc)
            except ValueError:
                await ctx.send(embed=_embed_simple(
                    "Invalid drop time -- expected ISO 8601 UTC, e.g. `2025-06-01T18:00:00`", _CLR_RED
                ))
                return

        d = float(delay_ms_str) if delay_ms_str else tuned_delay
        w = int(workers_str)    if workers_str   else int(cfg.get("workers", 5))

        if drop_time:
            eta_str = drop_time.strftime("%Y-%m-%d  %H:%M:%S UTC")
            desc    = f"Scheduled for **{eta_str}** with **{w}** workers, **{d:.1f}ms** delay."
        else:
            desc = f"Firing **immediately** with **{w}** workers, **{d:.1f}ms** delay."

        queue_embed = discord.Embed(
            title=f"Queued  --  {username}",
            description=desc,
            color=_CLR_BLUE,
            timestamp=datetime.now(timezone.utc),
        )
        queue_embed.set_footer(text="Repins Sniper")
        await ctx.send(embed=queue_embed)

        async def _job():
            nonlocal tuned_delay
            summary = await run_snipe(username, _tok(), drop_time, d, w, _sess())
            tuned_delay = summary.get("tuned_delay_ms", tuned_delay)
            embed = _embed_snipe_result(username, summary)
            await ctx.send(embed=embed)
            await _post_webhook(embed)

        active_jobs[username] = asyncio.create_task(_job())

    # -- !autosnipe -------------------------------------------

    @bot.command(name="autosnipe")
    async def cmd_autosnipe(
        ctx,
        username:     str = "",
        delay_ms_str: str = "",
        workers_str:  str = "",
    ):
        nonlocal tuned_delay
        if not username:
            await ctx.send(embed=_embed_simple(
                "Usage: `!autosnipe <username> [delay_ms] [workers]`", _CLR_RED
            ))
            return

        d = float(delay_ms_str) if delay_ms_str else tuned_delay
        w = int(workers_str)    if workers_str   else int(cfg.get("workers", 5))
        user = ctx.author

        old = active_jobs.pop(username, None)
        if old and not old.done():
            old.cancel()

        start_embed = discord.Embed(
            title=f"AutoSnipe  --  {username}",
            description="Checking status, then polling every 5 s until it drops.",
            color=_CLR_PURPLE,
            timestamp=datetime.now(timezone.utc),
        )
        start_embed.add_field(name="Workers", value=str(w),               inline=True)
        start_embed.add_field(name="Delay",   value=f"{d:.1f} ms",        inline=True)
        start_embed.add_field(name="Cancel",  value=f"`!cancel {username}`", inline=True)
        start_embed.set_footer(text="Repins Sniper")
        await ctx.send(embed=start_embed)

        async def _job():
            nonlocal tuned_delay
            sess = _sess()
            info = await scraper.fetch_drop_time(username, _tok(), session=sess)
            await ctx.send(embed=_embed_scrapetime(username, info))

            if info["status"] == "available":
                summary = await run_snipe(username, _tok(), delay_ms=d, workers=w, session=sess)

            elif info["status"] in ("taken", "unknown"):
                await ctx.send(embed=_embed_simple(
                    f"**{username}** is taken -- monitoring every 5 s, will fire the instant it drops.",
                    _CLR_GOLD,
                ))
                result = await scraper.poll_until_available(
                    username, _tok(), None, poll_interval=5.0, session=sess,
                )
                if result.status not in (NameStatus.AVAILABLE, NameStatus.FREE_404):
                    await ctx.send(embed=_embed_simple(
                        f"**{username}** ended with unexpected status `{result.status.value}`.",
                        _CLR_RED,
                    ))
                    return
                drop_embed = discord.Embed(
                    title=f"Name Available  --  {username}",
                    description=f"{user.mention}  Firing **{w}** workers now.",
                    color=_CLR_GREEN,
                    timestamp=datetime.now(timezone.utc),
                )
                drop_embed.set_footer(text="Repins Sniper")
                await ctx.send(embed=drop_embed)
                summary = await run_snipe(username, _tok(), delay_ms=d, workers=w, session=sess)

            else:
                await ctx.send(embed=_embed_simple(
                    f"**{username}** is `{info['status']}` -- cannot snipe.", _CLR_RED
                ))
                return

            tuned_delay = summary.get("tuned_delay_ms", tuned_delay)
            result_embed = _embed_snipe_result(username, summary)
            if summary["success"]:
                result_embed.description = f"{user.mention}"
            await ctx.send(embed=result_embed)
            await _post_webhook(result_embed)

        active_jobs[username] = asyncio.create_task(_job())

    # -- !superfastsnipe --------------------------------------

    @bot.command(name="superfastsnipe")
    async def cmd_superfastsnipe(ctx, username: str = ""):
        nonlocal tuned_delay
        if not username:
            await ctx.send(embed=_embed_simple(
                "Usage: `!superfastsnipe <username>`", _CLR_RED
            ))
            return

        fire_embed = discord.Embed(
            title=f"SuperFast Snipe  --  {username}",
            description=(
                f"{_SUPER_WORKERS} workers  .  0 ms delay  .  TCP pre-warmed\n"
                "Fires immediately. Use when the name is already available."
            ),
            color=_CLR_GOLD,
            timestamp=datetime.now(timezone.utc),
        )
        fire_embed.set_footer(text="Repins Sniper  *  Max performance mode")
        await ctx.send(embed=fire_embed)

        async def _job():
            nonlocal tuned_delay
            sess = _sess()
            if sess:
                await _warmup(sess, username)
            summary = await run_snipe(username, _tok(), delay_ms=0.0, workers=_SUPER_WORKERS, session=sess)
            tuned_delay = summary.get("tuned_delay_ms", tuned_delay)
            result_embed = _embed_snipe_result(username, summary)
            await ctx.send(embed=result_embed)
            await _post_webhook(result_embed)

        active_jobs[username] = asyncio.create_task(_job())

    # -- !scrapetime ------------------------------------------

    @bot.command(name="scrapetime")
    async def cmd_scrapetime(ctx, username: str = ""):
        if not username:
            await ctx.send(embed=_embed_simple(
                "Usage: `!scrapetime <username>`", _CLR_RED
            ))
            return

        thinking = await ctx.send(embed=_embed_simple(
            f"Querying Mojang, Ashcon, and PlayerDB for **{username}**...",
            _CLR_BLUE,
        ))
        try:
            info = await scraper.fetch_drop_time(username, _tok(), session=_sess())
        finally:
            try:
                await thinking.delete()
            except Exception:
                pass
        await ctx.send(embed=_embed_scrapetime(username, info))

    # -- !cancel ----------------------------------------------

    @bot.command(name="cancel")
    async def cmd_cancel(ctx, username: str = ""):
        if not username:
            await ctx.send(embed=_embed_simple("Usage: `!cancel <username>`", _CLR_RED))
            return
        task = active_jobs.pop(username, None)
        if task and not task.done():
            task.cancel()
            await ctx.send(embed=_embed_simple(f"Cancelled  --  **{username}**", _CLR_RED))
        else:
            await ctx.send(embed=_embed_simple(f"No active job found for **{username}**.", _CLR_GOLD))

    # -- !status ----------------------------------------------

    @bot.command(name="status")
    async def cmd_status(ctx):
        await ctx.send(embed=_embed_status(active_jobs))

    # -- !setdelay --------------------------------------------

    @bot.command(name="setdelay")
    async def cmd_setdelay(ctx, ms: str = ""):
        nonlocal tuned_delay
        if not ms:
            await ctx.send(embed=_embed_simple(
                f"Current delay: **{tuned_delay:.1f} ms**", _CLR_BLUE
            ))
            return
        try:
            tuned_delay = float(ms)
        except ValueError:
            await ctx.send(embed=_embed_simple("Invalid value -- expected a number.", _CLR_RED))
            return
        await ctx.send(embed=_embed_simple(
            f"Delay set to **{tuned_delay:.1f} ms**", _CLR_GREEN
        ))

    # -- !dropwatch -------------------------------------------

    @bot.command(name="dropwatch")
    async def cmd_dropwatch(ctx, action: str = "", length_str: str = "3"):
        """!dropwatch start [3|4]  |  !dropwatch stop  |  !dropwatch status"""
        action = action.lower().strip()

        if action == "start":
            if _watcher_holder and _watcher_holder[0].running:
                await ctx.send(embed=_embed_simple(
                    "DropWatcher is already running -- use `!dropwatch stop` first.",
                    _CLR_GOLD,
                ))
                return
            try:
                length = int(length_str)
                if length not in (3, 4):
                    raise ValueError
            except ValueError:
                await ctx.send(embed=_embed_simple(
                    "Length must be `3` or `4`.", _CLR_RED
                ))
                return

            sess = _sess()
            if sess is None:
                await ctx.send(embed=_embed_simple(
                    "Session not ready yet -- try again in a moment.", _CLR_RED
                ))
                return

            user = ctx.author

            async def _on_hit(r):
                # Announce the hit
                hit_embed = discord.Embed(
                    title=f"DropWatcher Hit  --  {r.name}",
                    description=f"{user.mention}  `{r.name}` is **AVAILABLE** -- auto-firing snipe now.",
                    color=_CLR_GREEN,
                    timestamp=datetime.now(timezone.utc),
                )
                hit_embed.set_footer(text="Repins Sniper  *  DropWatcher")
                await ctx.send(embed=hit_embed)

                # Fire a superfast snipe immediately
                try:
                    summary = await run_snipe(
                        r.name, _tok(),
                        delay_ms=0.0, workers=_SUPER_WORKERS, session=sess,
                    )
                    result_embed = _embed_snipe_result(r.name, summary)
                    if summary["success"]:
                        result_embed.description = f"{user.mention}"
                    await ctx.send(embed=result_embed)
                    await _post_webhook(result_embed)
                except Exception as exc:
                    await ctx.send(embed=_embed_simple(
                        f"Snipe failed for **{r.name}** -- `{exc}`", _CLR_RED
                    ))

            watcher = DropWatcher(
                session=sess,
                bearer_token=_tok(),
                length=length,
                on_hit=_on_hit,
            )
            _watcher_holder.clear()
            _watcher_holder.append(watcher)
            bot._dropwatcher = watcher  # type: ignore[attr-defined]
            watcher.start()

            pool_size = watcher.proxy_pool.size
            pool_line = (
                f"**{pool_size}** proxies loaded from `proxies.txt`"
                if pool_size
                else "No `proxies.txt` found -- running **direct** with 1 s throttle"
            )
            start_embed = discord.Embed(
                title=f"DropWatcher Started  --  {length}-char scan",
                description=pool_line,
                color=_CLR_PURPLE,
                timestamp=datetime.now(timezone.utc),
            )
            start_embed.add_field(name="Auto-Snipe", value=f"{_SUPER_WORKERS} workers, 0 ms delay", inline=True)
            start_embed.add_field(name="Stop",       value="`!dropwatch stop`",                    inline=True)
            start_embed.add_field(name="Status",     value="`!dropwatch status`",                  inline=True)
            start_embed.set_footer(text="Repins Sniper  *  DropWatcher")
            await ctx.send(embed=start_embed)

        elif action == "stop":
            if not _watcher_holder or not _watcher_holder[0].running:
                await ctx.send(embed=_embed_simple(
                    "DropWatcher is not running.", _CLR_GOLD
                ))
                return
            await _watcher_holder[0].stop()
            await ctx.send(embed=_embed_simple(
                "DropWatcher stopped.", _CLR_RED
            ))

        elif action == "status":
            if not _watcher_holder or not _watcher_holder[0].running:
                await ctx.send(embed=_embed_simple(
                    "DropWatcher is not running. Start it with `!dropwatch start [3|4]`.",
                    _CLR_GREY,
                ))
                return
            w = _watcher_holder[0]
            s = w.stats
            uptime_s = max(int(time.time() - s.started_at), 1)
            hours, rem = divmod(uptime_s, 3600)
            mins, secs = divmod(rem, 60)
            uptime = f"{hours}h {mins}m {secs}s"
            checks_per_sec = s.checks_total / uptime_s
            progress = (
                f"{s.current_cycle} / {s.cycle_size}"
                if s.cycle_size else "--"
            )

            status_embed = discord.Embed(
                title=f"DropWatcher Status  --  {w._length}-char scan",
                color=_CLR_BLUE,
                timestamp=datetime.now(timezone.utc),
            )
            status_embed.add_field(name="Uptime",       value=uptime,                                   inline=True)
            status_embed.add_field(name="Total Checks", value=f"{s.checks_total:,}",                    inline=True)
            status_embed.add_field(name="Rate",         value=f"{checks_per_sec:.1f} checks/s",         inline=True)
            status_embed.add_field(name="Cycle",        value=f"{s.cycles_done} done . {progress} now", inline=True)
            status_embed.add_field(name="Hits",         value=str(s.hits),                              inline=True)
            status_embed.add_field(
                name="Proxies",
                value=f"{w.proxy_pool.live_count} live / {w.proxy_pool.size} total",
                inline=True,
            )
            if s.last_hit_name:
                age = int(time.time() - s.last_hit_ts)
                status_embed.add_field(
                    name="Last Hit",
                    value=f"`{s.last_hit_name}` ({age}s ago)",
                    inline=False,
                )
            status_embed.set_footer(text="Repins Sniper  *  DropWatcher")
            await ctx.send(embed=status_embed)

        else:
            await ctx.send(embed=_embed_simple(
                "Usage: `!dropwatch start [3|4]` . `!dropwatch stop` . `!dropwatch status`",
                _CLR_BLUE,
            ))

    bot._discord_token = token  # type: ignore[attr-defined]
    return bot


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Entry point -- start bot, authenticate via Discord embed
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


async def _async_main() -> None:
    cfg = _load_config()

    # -- Try cached token (don't block -- auth happens via Discord embed) --
    cached = msauth.load_cached_token()
    bearer_ref: list[str] = [cached or ""]

    if cached:
        print(f"[startup] Loaded cached token -- ...{cached[-8:]}")
    else:
        print("[startup] No cached token -- will auth via Discord after bot starts.")

    # -- Pre-resolve DNS so the session skips it on every call -
    _resolve_host(_MC_SERVICES_HOST)

    bot = _build_bot(cfg, bearer_ref)
    if bot is None:
        print("[startup] No Discord token configured -- set DISCORD_BOT_TOKEN in .env")
        return

    print("[startup] Starting Discord bot -- all operations are command-driven.")
    try:
        await bot.start(bot._discord_token)  # type: ignore[attr-defined]
    except KeyboardInterrupt:
        pass
    finally:
        print("[startup] Shutting down...")
        watcher = getattr(bot, "_dropwatcher", None)
        if watcher is not None and watcher.running:
            try:
                await watcher.stop()
            except Exception:
                pass
        sess = getattr(bot, "_shared_session", None)
        if sess is not None and not sess.closed:
            try:
                await sess.close()
            except Exception:
                pass
        await bot.close()


def main() -> None:
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        print("\n[main] Interrupted -- exiting.")


if __name__ == "__main__":
    main()
