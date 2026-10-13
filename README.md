# Typeblasters Log

The analytics server for **The Typeblaster of the Dead** (CSCI 526). The game's WebGL
build sends gameplay events to it, and it stores every request exactly as it arrived,
including the player's IP address and browser headers, in a MySQL database.

```
game (GitHub Pages) ──HTTPS──> Cloudflare ──HTTP:80──> nginx ──> app.py (0.0.0.0:25630) ──> MySQL
                                                                     └──> data/raw-requests.jsonl
```

## Files

| File | What it is |
|---|---|
| `app.py` | The server (Flask, run by waitress). Creates the database tables on its first start. |
| `replay_raw_log.py` | Copies requests from the raw log into the database if they are missing there (e.g. after the database was down). |
| `requirements.txt` | Python packages. |
| `.env.example` | Settings template. Copy it to `.env` on the server and fill it in. |
| `nginx.conf.example` | Example nginx site for `typeblasterslogs.joey.ma`. |

`.env` (the database password) and `data/` (the raw log) are in `.gitignore`: they only
ever exist on the server. This repository is public.

## Running it on the server

Needs Python 3.9 or newer, and about 1 GB of memory for the container.

1. Upload `app.py`, `replay_raw_log.py`, `requirements.txt` and `.env.example`.
2. Copy `.env.example` to `.env` and fill in `DB_HOST`, `DB_NAME`, `DB_USER` and
   `DB_PASSWORD` (the password in single quotes).
3. Install the packages: `pip install -r requirements.txt`
4. Start it: `python app.py`

The console should show `Database OK: ...` and `Listening on 0.0.0.0:25630`. The first
start creates the two tables. If the database cannot be reached, the server still runs
and keeps everything in `data/raw-requests.jsonl`. Run `python replay_raw_log.py` once
the database works again.

## nginx and Cloudflare

- **nginx**: copy `nginx.conf.example` and replace `SERVER_IP`. No HTTPS, no
  http-to-https redirect, and no CORS headers in nginx (app.py adds them).
- **DNS**: an `A` record `typeblasterslogs` → the server's address, **Proxied** (orange cloud).
- **Always add a Configuration Rule** (Rules → Overview → Create rule → Configuration Rule):
  *Hostname equals `typeblasterslogs.joey.ma`* → *SSL: Flexible*. nginx only listens on
  port 80, so Cloudflare must talk to it over plain HTTP. Do this even if the zone's
  SSL/TLS mode shows *Automatic*: Cloudflare can raise that to *Full* on its own, and then
  it connects to port 443 and answers 521, 525 or 526 errors instead of passing data on.
- **Nothing may challenge or rate-limit this hostname**: no "I'm Under Attack" mode, no
  WAF or rate-limiting rules for it, and Bot Fight Mode off (Security → Settings; on the
  Free plan it is one switch for the whole joey.ma zone and cannot be skipped for one
  hostname). A game's background request cannot solve a challenge page, and about 90
  classmates on the campus Wi-Fi share a few IP addresses, which can look like an attack.
- **Optional**: make port 25630 reachable only from nginx, by putting the panel
  allocation on `127.0.0.1`. (A `ufw` rule does not work for a Docker container's
  published port; it would have to go in Docker's `DOCKER-USER` chain.) Check from another
  computer that `SERVER_IP:25630` no longer answers. Then nobody can skip nginx, so
  `peer_ip` and `x_real_ip` can be trusted; `cf_connecting_ip` (and so `client_ip`) can
  still be made up by someone who skips Cloudflare. No player is turned away by this,
  because real traffic always comes through Cloudflare and nginx.

## Checking that it works

- Open `https://typeblasterslogs.joey.ma/health` in a browser. It should show
  `{"ok": true, "database": true, "raw_log": true}`. `raw_log` is false when the data
  folder cannot be written or has less than 200 MB free. This is also the address for an
  uptime monitor; health checks are not stored.
- Send a test batch. In bash (Linux, macOS, Git Bash):

  ```bash
  curl -i -X POST https://typeblasterslogs.joey.ma/v1/batch -H "Content-Type: text/plain" --data "{\"format\":1,\"install_id\":\"qa-curl\",\"events\":[{\"event\":\"test\",\"event_id\":\"qa-1\",\"seq\":1}]}"
  ```

  In Windows PowerShell:

  ```powershell
  Invoke-RestMethod -Method Post -Uri https://typeblasterslogs.joey.ma/v1/batch -ContentType 'text/plain' -Body '{"format":1,"install_id":"qa-ps","events":[{"event":"test","event_id":"qa-2","seq":1}]}'
  ```

  The reply should say `"ok": true`, and a new row should appear in both tables. Test rows
  use `install_id` values starting with `qa-`, so they are easy to delete before the exam.

## What the game sends (format 1)

```
POST https://typeblasterslogs.joey.ma/v1/batch
Content-Type: text/plain
```

The body is JSON. `text/plain` keeps it a "simple" request for the browser, so no extra
permission request (CORS preflight) happens first. There are no keys, cookies or logins.

```json
{
  "format": 1,
  "batch_id": "6f1c...",
  "install_id": "b2a9...",
  "session_id": "41d0...",
  "build": "7ade776",
  "events": [
    {"event": "run_start", "event_id": "41d0...-12", "seq": 12, "run_id": "9c3e...",
     "level": "normal", "wave": 0, "t_play": 0.0, "t_real": 31.4, "tips_enabled": true},
    {"event": "damage", "event_id": "41d0...-13", "seq": 13, "run_id": "9c3e...",
     "level": "normal", "wave": 1, "t_play": 22.8, "t_real": 54.2, "source": "runner", "amount": 10}
  ]
}
```

- These 12 fields get their own columns in `events`: `event`, `event_id`, `batch_id`,
  `install_id`, `session_id`, `run_id`, `seq`, `build`, `level`, `wave`, `t_play`,
  `t_real`. Written next to `events`, they apply to every event in the batch (an event
  can override them).
- Every other field inside an event is kept in that event's `data` column. Other fields
  next to `events` (like `format`) are only in `requests.body`; join on `request_id`.
- A bare list of events, or a single event object, is also accepted.
- Keep each batch small: about 100 events and well under 1 MiB. A request over 1 MiB is
  refused by nginx before app.py sees it: it is not stored, and the game only sees a
  network error, which looks the same as the server being down. So the game must split a
  big batch before sending it, never build one near 1 MiB, and never resend an oversized
  one unchanged. Other limits: 1000 events per batch, JSON nested at most 31 levels deep.

Replies:

| Status | Meaning | What the game should do |
|---|---|---|
| 200 | Stored | Drop the batch. |
| 400 | Body refused: not UTF-8, not JSON, not an object or list, or over a limit (the raw body is stored anyway, see `parse_error`) | Drop the batch; sending it again won't help. |
| 503 | Could not store it anywhere | Keep the batch, send it again later with the same `batch_id`. |
| no reply / network error | Server or connection down | Same as 503. |

A batch that is sent twice is stored twice. Remove duplicates during the analysis by
`event_id`.

## The tables

**`requests`**: one row per HTTP request, except `GET` and `HEAD /health`. Gameplay rows
are `method = 'POST' AND path = '/v1/batch' AND parse_ok = 1`; everything else (404s,
`OPTIONS` preflights with status 204, visits to `/` with status 200, refused bodies) is
not gameplay.

| Column | Contents |
|---|---|
| `received_at` | Server time when it arrived, **UTC**. |
| `client_ip` | Best guess of the player's IP: `CF-Connecting-IP`, else `X-Real-IP`, else the first `X-Forwarded-For`, else `peer_ip`. |
| `peer_ip` | The address that opened the connection to app.py (normally nginx). |
| `cf_connecting_ip`, `x_real_ip`, `x_forwarded_for`, `cf_ray`, `cf_ipcountry` | The raw proxy headers. |
| `user_agent`, `origin`, `referer`, `sec_fetch_*`, `sec_ch_ua*`, `accept_language`, `content_type` | Common browser headers. |
| `headers` | Every header, as JSON. |
| `body`, `body_encoding`, `body_bytes` | The body exactly as received (base64 if it was not UTF-8 text). |
| `status`, `parse_ok`, `parse_error`, `event_count` | What the server replied and how many events it found. |
| `source` | `live`, or `replay` when added by `replay_raw_log.py`. |
| `raw_id` | Links the row to its line in `data/raw-requests.jsonl`. |

**`events`**: one row per event. `request_id` points to its row in `requests`, and `data`
is the event object as parsed (the untouched bytes are in `requests.body`).

```sql
-- How many of each event
SELECT event, COUNT(*) FROM events GROUP BY event ORDER BY COUNT(*) DESC;

-- Events together with the IP address and browser they came from
SELECT e.*, r.client_ip, r.user_agent, r.origin
FROM events e JOIN requests r ON r.id = e.request_id;
```

## Spotting fake traffic

Nothing is blocked. Everything is kept, so suspicious data can be filtered out later.
Check in this order:

1. **Skipped nginx**: `peer_ip` is not the address that almost every row shares (nginx's;
   see it on your own test request). Such a request went straight to port 25630, so every
   header on it may be made up, including `cf_ray`, `x_real_ip`, `cf_connecting_ip` and
   therefore `client_ip`.
2. **Skipped Cloudflare**: `x_real_ip` is not a Cloudflare address
   (https://www.cloudflare.com/ips/). The request went straight to nginx, so
   `cf_connecting_ip` and `cf_ray` may be made up. (`cf_ray` alone proves nothing.)
3. **Not from the game page**: `origin` is none of the game's pages
   (`https://bunfloof.github.io`, or one of the team's Vercel build links
   `https://...vercel.app`), or the `sec_fetch_*` headers are missing. Browsers always set
   these themselves; scripts usually don't.
4. **Impossible gameplay**: after removing duplicate `event_id`s (normal retries), the
   same `seq` with different `event_id`s, or `seq` running backwards, within a
   `session_id`; unknown `build` values; numbers the game cannot produce. A gap in `seq`
   alone can just be a lost batch.
5. **Not gameplay at all**: see the gameplay-row rule under "The tables". 404 rows are
   often internet scanners; `parse_ok = 0` rows are refused bodies (`parse_error` says why).

## Before and during the exam

- Back up and then empty both tables, so test data is not mixed in. Keep a copy of
  `data/raw-requests.jsonl` and start a new one.
- During the exam, back up every 10 to 15 minutes **without locking the tables**
  (`mariadb-dump` takes the same options):

  ```bash
  mysqldump --single-transaction --quick --no-tablespaces -h DB_HOST -u DB_USER -p DB_NAME > backup-$(date +%H%M).sql
  ```

  A plain `mysqldump` locks the tables while it runs, which stalls every request. Also copy
  `data/raw-requests.jsonl`.
- Watch `/health` and the free disk space.
