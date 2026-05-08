# Repins Eman

Hey Utsab, you useless fuck. You couldn't build a sniper if your life depended on it so I did it for you. Again. You're welcome. Don't thank me. Actually don't even talk to me.

It's fully automated now. One command. It authenticates itself. It runs forever. You don't have to type anything. I literally made it idiot-proof for you specifically.

---

## What this does, since you clearly still an't read code

Steals Minecraft usernames before your two braincells can register the name even dropped. Sub-millisecond precision. Async workers. Spin-loop timing. TCP pre-warmed. Auto-tuned delay. Discord bot. Rich embeds. The works.

No NameMC. No third-party crap. Pure Mojang endpoints only. You're welcome, again.

---

## Setup (read this slowly, I know you struggle)

**1. Install dependencies**
```bash
pip install -r requirements.txt
```

**2. Put your tokens in `config.json`** (it's already there, don't delete it you animal)
```json
{
  "discord_token": "YOUR_BOT_TOKEN",
  "webhook_url":   "YOUR_WEBHOOK_URL",
  "account_type":  "ms",
  "workers":       5,
  "delay":         0
}
```

**3. Run it**
```bash
python main.py
```

It opens your browser, you log in with Microsoft, it caches the token for **23 hours** and never asks again. If it's already cached it just starts instantly. That's it. Stop asking questions.

---

## Discord bot commands (I know you'll forget these too)

| Command | What it does |
|---|---|
| `!snipe <name> [drop_time] [delay_ms] [workers]` | Snipe a name now, or schedule it for a UTC ISO drop time |
| `!autosnipe <name> [delay_ms] [workers]` | Polls every 5s until it drops, then fires automatically |
| `!superfastsnipe <name>` | 10 workers, 0ms delay, TCP pre-warmed. Instant. Don't think, just use it |
| `!scrapetime <name>` | Checks availability + holder UUID + how long they've had it. Actually accurate |
| `!dropwatch start [3\|4]` | Mass-scan every 3 or 4-char name against Mojang forever. Auto-fires superfastsnipe on any hit |
| `!dropwatch stop` | Stop the mass scan |
| `!dropwatch status` | Live stats: uptime, checks/sec, live proxies, last hit |
| `!cancel <name>` | Stop stalking that name |
| `!status` | See what's running |
| `!setdelay [ms]` | Override the auto-tuned delay. Or don't pass a value to see what it is |
| `!help` | Sends you this info in Discord since you'll lose this README anyway |

---

## How the timing works (since you'll ask)

1. Resolves DNS once at startup — no DNS overhead per request
2. Warms up the TCP connection 10s before the scheduled drop
3. Sleeps until 200ms before fire time (saves CPU)
4. Spin-loops on `perf_counter_ns` for the final 200ms — sub-millisecond accuracy
5. All workers fire at the exact same nanosecond
6. Auto-tunes the delay offset after every timed snipe

It's fast. Faster than you deserve.

---

## DropWatcher (mass scanning with proxies)

`!dropwatch start` continuously hammers the Mojang API for every possible 3-char
(or 4-char) username and auto-snipes anything that drops. Concurrency is limited
only by how many proxies you give it.

**To use proxies** — rename `proxies.txt.example` to `proxies.txt` and fill it
with one proxy per line:

```
192.0.2.10:8080
user:pass@192.0.2.11:8080
http://user:pass@proxy.example.com:8080
```

> HTTP/HTTPS proxies only. `aiohttp` doesn't do SOCKS natively. If you really
> need SOCKS, install `aiohttp-socks` and open an issue — or, y'know, just use
> HTTP proxies like a normal person.

**Rate-limit behavior:**
- 429 response → that proxy is cooled down for 30 s
- Connection error → proxy cooled down for 60 s
- All proxies banned → waits 5 s and retries
- **No proxies at all** → falls back to direct, throttled to ~1 check/sec

**Pool size recommendations:**
- 50-100 residential proxies → comfortable 3-char coverage
- 500+ → aggressive 4-char (37⁴ = 1.87M names per cycle)
- Datacenter proxies work but get banned fast, expect a high ban rate

---

## FAQ

**Q: How do I use this?**
A: There's a whole section above. Literacy is free.

**Q: It's not working!**
A: Skill issue.

**Q: The token expired!**
A: It auto-refreshes when you restart. Restart it, genius.

**Q: Can you add [feature]?**
A: No.

**Q: Why doesn't `!scrapetime` show the exact drop time like NameMC?**
A: Because NameMC is lying to you. Mojang removed the 37-day name-change
cooldown in September 2022. Names drop the **instant** the holder changes
theirs — there's no scheduled drop to predict. The +37d math NameMC used to do
is obsolete. The only way to catch a drop now is to poll the Mojang API
continuously, which is exactly what `!dropwatch` does.

**Q: How many proxies do I need for `!dropwatch`?**
A: For 3-char scanning, 50-100 residential proxies is comfortable. For 4-char
(1.87 million names per cycle), you want 500+. Without proxies it falls back
to 1 check/sec — which is fine for testing but useless for catching fast drops.

**Q: All my proxies got banned!**
A: Skill issue. Get residential proxies, not the free list you scraped off some
sketchy pastebin.
