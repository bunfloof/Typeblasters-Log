"""
Typeblasters Log: the analytics server for "The Typeblaster of the Dead".

The Unity WebGL game sends its gameplay events here, a few at a time, as JSON:

    POST /v1/batch     Content-Type: text/plain, body = JSON (see README.md)

Every request is stored exactly as it arrived, in two places:

  1. data/raw-requests.jsonl - one line per request, written FIRST, so nothing
                               is lost while the database is down
                               (replay_raw_log.py copies missing lines into
                               the database later);
  2. the MySQL database      - one row per request in `requests` (IP
                               addresses, every header, the untouched body)
                               and one row per event in `events`.

Nothing is turned away for looking suspicious: there are no keys, cookies or
logins. Bad data is filtered out afterwards, during the analysis.

Start it with:   python app.py      (settings come from the .env file)
"""
import base64
import json
import math
import os
import shutil
import threading
import uuid
from datetime import datetime, timezone

import pymysql
from dotenv import load_dotenv
from flask import Flask, Response, request
from waitress import serve


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

APP_FOLDER = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(APP_FOLDER, ".env"))

DB_HOST = os.environ.get("DB_HOST", "")
DB_PORT = int(os.environ.get("DB_PORT") or "3306")
DB_NAME = os.environ.get("DB_NAME", "")
DB_USER = os.environ.get("DB_USER", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

LISTEN_HOST = os.environ.get("LISTEN_HOST") or "0.0.0.0"
LISTEN_PORT = int(os.environ.get("LISTEN_PORT") or "25630")

BATCH_PATH = "/v1/batch"
HEALTH_PATH = "/health"

RAW_LOG_FOLDER = os.path.join(APP_FOLDER, "data")
RAW_LOG_PATH = os.path.join(RAW_LOG_FOLDER, "raw-requests.jsonl")

MAX_BODY_BYTES = 1024 * 1024   # 1 MiB, the same as nginx's default client_max_body_size
MAX_EVENTS_PER_BATCH = 1000    # the game sends far fewer; stops one request turning into 100,000 rows
MAX_JSON_DEPTH = 31            # MariaDB refuses JSON with 32 or more nested {} or [] (MySQL allows 100)
MAX_JSON_KEY_BYTES = 65535     # MySQL refuses a JSON object key longer than this
WORKER_THREADS = 16            # how many requests are handled at the same time
CONNECTION_LIMIT = 500         # open connections waitress accepts (its default is 100)
MIN_FREE_DISK_BYTES = 200 * 1024 * 1024  # /health reports a problem below 200 MB free

BIGGEST_INT64 = 2 ** 63 - 1
BIGGEST_INT32 = 2 ** 31 - 1


# ---------------------------------------------------------------------------
# Database tables (created automatically on the first start)
# ---------------------------------------------------------------------------

# One row per HTTP request that reached the server (all except GET /health).
CREATE_REQUESTS_TABLE = """
CREATE TABLE IF NOT EXISTS `requests` (
    `id`                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `raw_id`             CHAR(32)        NOT NULL,
    `source`             VARCHAR(16)     NOT NULL,
    `received_at`        DATETIME(6)     NOT NULL,
    `method`             VARCHAR(16)     NOT NULL,
    `path`               MEDIUMTEXT      NOT NULL,
    `query_string`       MEDIUMTEXT      NOT NULL,
    `status`             SMALLINT        NOT NULL,
    `client_ip`          VARCHAR(64)     NULL,
    `peer_ip`            VARCHAR(64)     NULL,
    `cf_connecting_ip`   VARCHAR(64)     NULL,
    `x_real_ip`          VARCHAR(64)     NULL,
    `x_forwarded_for`    VARCHAR(512)    NULL,
    `cf_ray`             VARCHAR(64)     NULL,
    `cf_ipcountry`       VARCHAR(8)      NULL,
    `user_agent`         VARCHAR(1024)   NULL,
    `origin`             VARCHAR(512)    NULL,
    `referer`            VARCHAR(1024)   NULL,
    `sec_fetch_site`     VARCHAR(32)     NULL,
    `sec_fetch_mode`     VARCHAR(32)     NULL,
    `sec_fetch_dest`     VARCHAR(32)     NULL,
    `sec_ch_ua`          VARCHAR(512)    NULL,
    `sec_ch_ua_platform` VARCHAR(64)     NULL,
    `sec_ch_ua_mobile`   VARCHAR(8)      NULL,
    `accept_language`    VARCHAR(256)    NULL,
    `content_type`       VARCHAR(256)    NULL,
    `headers`            JSON            NOT NULL,
    `body`               MEDIUMTEXT      NOT NULL,
    `body_encoding`      VARCHAR(16)     NOT NULL,
    `body_bytes`         INT UNSIGNED    NOT NULL,
    `parse_ok`           TINYINT(1)      NULL,
    `parse_error`        VARCHAR(512)    NULL,
    `event_count`        INT UNSIGNED    NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_requests_raw_id` (`raw_id`),
    KEY `ix_requests_received_at` (`received_at`),
    KEY `ix_requests_client_ip` (`client_ip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

# One row per event inside a POST /v1/batch. `data` is the event object as
# parsed (the untouched bytes are in requests.body); the other columns are
# copies of common fields, so they are easy to filter and sort on.
CREATE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS `events` (
    `id`                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `request_id`        BIGINT UNSIGNED NOT NULL,
    `position_in_batch` INT UNSIGNED    NOT NULL,
    `received_at`       DATETIME(6)     NOT NULL,
    `event`             VARCHAR(64)     NULL,
    `event_id`          VARCHAR(64)     NULL,
    `batch_id`          VARCHAR(64)     NULL,
    `install_id`        VARCHAR(64)     NULL,
    `session_id`        VARCHAR(64)     NULL,
    `run_id`            VARCHAR(64)     NULL,
    `seq`               BIGINT          NULL,
    `build`             VARCHAR(128)    NULL,
    `level`             VARCHAR(32)     NULL,
    `wave`              INT             NULL,
    `t_play`            DOUBLE          NULL,
    `t_real`            DOUBLE          NULL,
    `data`              JSON            NOT NULL,
    PRIMARY KEY (`id`),
    KEY `ix_events_request_id` (`request_id`),
    KEY `ix_events_event` (`event`),
    KEY `ix_events_event_id` (`event_id`),
    KEY `ix_events_install_id` (`install_id`),
    KEY `ix_events_session_seq` (`session_id`, `seq`),
    KEY `ix_events_run_id` (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

EVENT_COLUMNS = [
    "request_id", "position_in_batch", "received_at", "event", "event_id",
    "batch_id", "install_id", "session_id", "run_id", "seq", "build", "level",
    "wave", "t_play", "t_real", "data",
]


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def log(message):
    """Prints one line to the console, with the time in UTC. flush=True makes
    the line show up right away in a container's console."""
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(stamp + " " + message, flush=True)


def describe(error):
    """A short one-line description of an error, for the console."""
    text = type(error).__name__ + ": " + str(error)
    return text.replace("\n", " ")[:300]


def clean_text(text):
    """MySQL cannot store "lone surrogate" characters, which a sender can
    smuggle in with JSON escapes like \\ud800. They become "?"."""
    return text.encode("utf-8", "replace").decode("utf-8")


def as_text(value, max_length=None):
    """Any JSON value as text for a text column, cut to max_length characters
    (MySQL would refuse a longer value). None stays None."""
    if value is None:
        return None
    if isinstance(value, str):
        text = value
    else:
        text = json.dumps(value, ensure_ascii=False)
    text = clean_text(text)
    if max_length is not None:
        text = text[:max_length]
    return text


def as_integer(value, biggest):
    """A whole number between -biggest-1 and biggest, or None when the value
    is not one (true/false, text, fractions and huge numbers all give None)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        number = value
    elif isinstance(value, float) and math.isfinite(value) and value == int(value):
        number = int(value)
    elif isinstance(value, str):
        try:
            number = int(value.strip())
        except ValueError:
            return None
    else:
        return None
    if number < -biggest - 1 or number > biggest:
        return None
    return number


def as_number(value):
    """A finite decimal number, or None when the value is not one."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number):
        return None
    return number


# ---------------------------------------------------------------------------
# Reading the JSON the game sends
# ---------------------------------------------------------------------------

def not_a_number_to_none(name):
    """json.loads calls this for NaN, Infinity and -Infinity. They are not
    real JSON and MySQL refuses them, so they become null."""
    return None


def decimal_or_none(text):
    """json.loads calls this for every decimal number, like "1.5". A number
    too big for a float (like 1e999) would become infinity, so it becomes
    null instead."""
    number = float(text)
    if math.isfinite(number):
        return number
    return None


def whole_number_or_text(text):
    """json.loads calls this for every whole number. A number outside the
    64-bit range is kept as text, so MySQL can still store it. (Longer than
    20 characters is always outside it; checking that first also avoids
    Python's error for numbers with more than 4300 digits.)"""
    if len(text) > 20:
        return text
    number = int(text)
    if -BIGGEST_INT64 - 1 <= number <= BIGGEST_INT64:
        return number
    return text


def json_shape_problem(value):
    """Checks parsed JSON for things the database would refuse: too many
    levels of {} and [] inside each other, or an object key that is too
    long. Returns a short message, or None when the JSON is fine."""
    to_visit = [(value, 1)]
    while to_visit:
        item, depth = to_visit.pop()
        if isinstance(item, dict):
            children = item.values()
            for key in item:
                if len(key.encode("utf-8", "replace")) > MAX_JSON_KEY_BYTES:
                    return "a JSON key is longer than " + str(MAX_JSON_KEY_BYTES) + " bytes"
        elif isinstance(item, list):
            children = item
        else:
            continue
        if depth > MAX_JSON_DEPTH:
            return "JSON is nested more than " + str(MAX_JSON_DEPTH) + " levels deep"
        for child in children:
            to_visit.append((child, depth + 1))
    return None


def parse_batch(text):
    """Reads the body of a POST /v1/batch.

    Returns (batch_fields, events, error); error is None when it worked.
    These shapes are accepted:
        {"install_id": "...", "events": [{...}, {...}]}   the normal one
        [{...}, {...}]                                    only a list of events
        {...}                                             one single event
    """
    if text.startswith("\ufeff"):  # a "byte order mark" that some tools add
        text = text[1:]
    try:
        parsed = json.loads(
            text,
            parse_constant=not_a_number_to_none,
            parse_float=decimal_or_none,
            parse_int=whole_number_or_text,
        )
    except (ValueError, RecursionError) as error:
        return {}, [], "not valid JSON: " + describe(error)

    problem = json_shape_problem(parsed)
    if problem is not None:
        return {}, [], problem

    if isinstance(parsed, dict) and isinstance(parsed.get("events"), list):
        batch_fields = parsed
        items = parsed["events"]
    elif isinstance(parsed, list):
        batch_fields = {}
        items = parsed
    elif isinstance(parsed, dict):
        batch_fields = {}
        items = [parsed]
    else:
        return {}, [], "JSON must be an object or a list"

    if len(items) > MAX_EVENTS_PER_BATCH:
        return {}, [], (str(len(items)) + " events in one batch, the limit is "
                        + str(MAX_EVENTS_PER_BATCH))

    events = []
    for item in items:
        if isinstance(item, dict):
            events.append(item)
        else:
            events.append({"value": item})
    return batch_fields, events, None


# ---------------------------------------------------------------------------
# One request = one "record"
# ---------------------------------------------------------------------------

def read_request():
    """Turns the incoming HTTP request into a plain dictionary (a "record").
    It is saved unchanged in the raw log, so the replay script can store it
    in the database later in exactly the same way."""
    received_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    body = request.get_data(cache=False)
    try:
        body_text = body.decode("utf-8")
        body_encoding = "utf-8"
    except UnicodeDecodeError:
        body_text = base64.b64encode(body).decode("ascii")
        body_encoding = "base64"

    headers = {}
    for name, value in request.headers.items():
        headers[name.lower()] = value

    return {
        "raw_id": uuid.uuid4().hex,
        "received_at": received_at,            # UTC
        "method": request.method,
        "path": request.path,
        "query_string": request.query_string.decode("latin-1"),
        "peer_ip": request.remote_addr or "",  # whoever opened the connection (nginx, or a direct visitor)
        "headers": headers,
        "body": body_text,
        "body_encoding": body_encoding,
        "body_bytes": len(body),
    }


def understand_request(record):
    """Works out the reply status and the events of a record. The live server
    and replay_raw_log.py both use this, so they always agree."""
    outcome = {
        "status": 404,
        "parse_ok": None,     # None = not a POST /v1/batch
        "parse_error": None,
        "batch_fields": {},
        "events": [],
    }
    method = record["method"]
    path = record["path"]

    if method == "OPTIONS":
        # A browser asking for permission first (a CORS "preflight"). The
        # game sends text/plain to avoid these, so they should be rare.
        outcome["status"] = 204
    elif method == "POST" and path == BATCH_PATH:
        if record["body_encoding"] == "utf-8":
            batch_fields, events, error = parse_batch(record["body"])
        else:
            batch_fields, events, error = {}, [], "body is not UTF-8 text"
        if error is None:
            outcome["status"] = 200
            outcome["parse_ok"] = 1
            outcome["batch_fields"] = batch_fields
            outcome["events"] = events
        else:
            outcome["status"] = 400
            outcome["parse_ok"] = 0
            outcome["parse_error"] = error
    elif path == "/" and method in ("GET", "HEAD"):
        outcome["status"] = 200
    return outcome


# ---------------------------------------------------------------------------
# Raw log file (the safety net)
# ---------------------------------------------------------------------------

raw_log_lock = threading.Lock()


def raw_log_ends_with_newline():
    """False when the last line of the raw log was cut off, e.g. because the
    server was killed or the disk filled up halfway through a write."""
    try:
        with open(RAW_LOG_PATH, "rb") as file:
            file.seek(0, os.SEEK_END)
            if file.tell() == 0:
                return True
            file.seek(-1, os.SEEK_END)
            return file.read(1) == b"\n"
    except FileNotFoundError:
        return True


def append_raw_log(record):
    """Adds the record as one line to data/raw-requests.jsonl. Returns True
    when it worked. ensure_ascii=True keeps the file plain ASCII, so writing
    it can never fail because of an unusual character."""
    try:
        line = json.dumps(record, ensure_ascii=True) + "\n"
        with raw_log_lock:
            os.makedirs(RAW_LOG_FOLDER, exist_ok=True)
            if not raw_log_ends_with_newline():
                # Start on a fresh line, so a cut-off line cannot swallow this one.
                line = "\n" + line
            with open(RAW_LOG_PATH, "a", encoding="utf-8") as file:
                file.write(line)
        return True
    except Exception as error:
        log("Raw log write failed (" + record["raw_id"] + "): " + describe(error))
        return False


def raw_log_writable():
    """True when the raw log's folder can be written to and has enough room."""
    try:
        os.makedirs(RAW_LOG_FOLDER, exist_ok=True)
        if not os.access(RAW_LOG_FOLDER, os.W_OK):
            return False
        return shutil.disk_usage(RAW_LOG_FOLDER).free >= MIN_FREE_DISK_BYTES
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

# Every worker thread keeps its own database connection and reuses it.
thread_data = threading.local()

tables_ready = False
tables_lock = threading.Lock()


def get_connection():
    connection = getattr(thread_data, "connection", None)
    if connection is not None:
        try:
            connection.ping()  # still connected?
            return connection
        except Exception:
            forget_connection()  # MySQL closed it; open a new one below
    connection = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        autocommit=False,
        # Short time limits: while the database is stuck, the request is
        # already safe in the raw log and should not wait long for MySQL.
        connect_timeout=5,
        read_timeout=5,
        write_timeout=5,
    )
    thread_data.connection = connection
    return connection


def forget_connection():
    """Closes this thread's connection after an error; the next request
    opens a fresh one."""
    connection = getattr(thread_data, "connection", None)
    thread_data.connection = None
    if connection is not None:
        try:
            connection.close()
        except Exception:
            pass


def ensure_tables(connection):
    """Creates the two tables if they do not exist yet (once per start)."""
    global tables_ready
    if tables_ready:
        return
    with tables_lock:
        if tables_ready:
            return
        with connection.cursor() as cursor:
            cursor.execute(CREATE_REQUESTS_TABLE)
            cursor.execute(CREATE_EVENTS_TABLE)
        connection.commit()
        tables_ready = True


def pick_client_ip(headers, peer_ip):
    """Best guess of the player's IP address:
    1. CF-Connecting-IP - set by Cloudflare;
    2. X-Real-IP        - set by nginx when there is no Cloudflare in front;
    3. the first address in X-Forwarded-For;
    4. the address that opened the connection.
    Anyone who skips Cloudflare can fake 1 to 3, so the raw values are stored
    as well (see README.md, "Spotting fake traffic")."""
    for name in ("cf-connecting-ip", "x-real-ip"):
        value = headers.get(name, "").strip()
        if value:
            return value
    first_forwarded = headers.get("x-forwarded-for", "").split(",")[0].strip()
    if first_forwarded:
        return first_forwarded
    return peer_ip


def header_text(headers, name, max_length):
    return as_text(headers.get(name), max_length)


def headers_as_json(headers):
    """All headers as JSON text for the `headers` column. Names are cut to
    1000 characters, because MySQL refuses a JSON key over 64 KB (real
    header names are short; the raw log keeps them whole)."""
    short_names = {}
    for name, value in headers.items():
        short_names[name[:1000]] = value
    return clean_text(json.dumps(short_names, ensure_ascii=False))


def insert_request_row(cursor, record, outcome, source):
    """Adds the row in `requests` and returns its id."""
    headers = record["headers"]
    columns = [
        ("raw_id", record["raw_id"]),
        ("source", source),
        ("received_at", record["received_at"]),
        ("method", as_text(record["method"], 16)),
        ("path", as_text(record["path"])),
        ("query_string", as_text(record["query_string"])),
        ("status", outcome["status"]),
        ("client_ip", as_text(pick_client_ip(headers, record["peer_ip"]), 64)),
        ("peer_ip", as_text(record["peer_ip"], 64)),
        ("cf_connecting_ip", header_text(headers, "cf-connecting-ip", 64)),
        ("x_real_ip", header_text(headers, "x-real-ip", 64)),
        ("x_forwarded_for", header_text(headers, "x-forwarded-for", 512)),
        ("cf_ray", header_text(headers, "cf-ray", 64)),
        ("cf_ipcountry", header_text(headers, "cf-ipcountry", 8)),
        ("user_agent", header_text(headers, "user-agent", 1024)),
        ("origin", header_text(headers, "origin", 512)),
        ("referer", header_text(headers, "referer", 1024)),
        ("sec_fetch_site", header_text(headers, "sec-fetch-site", 32)),
        ("sec_fetch_mode", header_text(headers, "sec-fetch-mode", 32)),
        ("sec_fetch_dest", header_text(headers, "sec-fetch-dest", 32)),
        ("sec_ch_ua", header_text(headers, "sec-ch-ua", 512)),
        ("sec_ch_ua_platform", header_text(headers, "sec-ch-ua-platform", 64)),
        ("sec_ch_ua_mobile", header_text(headers, "sec-ch-ua-mobile", 8)),
        ("accept_language", header_text(headers, "accept-language", 256)),
        ("content_type", header_text(headers, "content-type", 256)),
        ("headers", headers_as_json(headers)),
        ("body", record["body"]),
        ("body_encoding", record["body_encoding"]),
        ("body_bytes", record["body_bytes"]),
        ("parse_ok", outcome["parse_ok"]),
        ("parse_error", as_text(outcome["parse_error"], 512)),
        ("event_count", len(outcome["events"])),
    ]
    names = ", ".join("`" + name + "`" for name, value in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    values = [value for name, value in columns]
    cursor.execute("INSERT INTO `requests` (" + names + ") VALUES (" + placeholders + ")", values)
    return cursor.lastrowid


def pick(event, batch_fields, name):
    """A field from the event itself, or else from the batch around it."""
    if name in event:
        return event[name]
    return batch_fields.get(name)


def insert_event_rows(cursor, request_id, record, outcome):
    """Adds one row in `events` for every event of the batch."""
    batch_fields = outcome["batch_fields"]
    rows = []
    position = 0
    for event in outcome["events"]:
        rows.append([
            request_id,
            position,
            record["received_at"],
            as_text(pick(event, batch_fields, "event"), 64),
            as_text(pick(event, batch_fields, "event_id"), 64),
            as_text(pick(event, batch_fields, "batch_id"), 64),
            as_text(pick(event, batch_fields, "install_id"), 64),
            as_text(pick(event, batch_fields, "session_id"), 64),
            as_text(pick(event, batch_fields, "run_id"), 64),
            as_integer(pick(event, batch_fields, "seq"), BIGGEST_INT64),
            as_text(pick(event, batch_fields, "build"), 128),
            as_text(pick(event, batch_fields, "level"), 32),
            as_integer(pick(event, batch_fields, "wave"), BIGGEST_INT32),
            as_number(pick(event, batch_fields, "t_play")),
            as_number(pick(event, batch_fields, "t_real")),
            clean_text(json.dumps(event, ensure_ascii=False)),
        ])
        position += 1
    if not rows:
        return
    names = ", ".join("`" + name + "`" for name in EVENT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(EVENT_COLUMNS))
    cursor.executemany("INSERT INTO `events` (" + names + ") VALUES (" + placeholders + ")", rows)


def save_to_database(record, outcome, source):
    """Stores the record and its events in one transaction. Returns True when
    it worked. Never raises: a broken database must not break the server,
    because the raw log already holds the data."""
    try:
        connection = get_connection()
        ensure_tables(connection)
        with connection.cursor() as cursor:
            request_id = insert_request_row(cursor, record, outcome, source)
            insert_event_rows(cursor, request_id, record, outcome)
        connection.commit()
        return True
    except Exception as error:
        log("Database write failed (" + record["raw_id"] + "): " + describe(error))
        forget_connection()
        return False


# ---------------------------------------------------------------------------
# The web server
# ---------------------------------------------------------------------------

# static_folder=None: this server has no files to serve.
app = Flask(__name__, static_folder=None)


def json_reply(status, data):
    return Response(json.dumps(data), status=status, mimetype="application/json")


@app.after_request
def add_cors_headers(response):
    """Lets the game's page (on another site, GitHub Pages) send requests
    here and read the reply. "*" is fine because no cookies or logins are
    used, and CORS never stops scripts outside a browser anyway."""
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    asked_for = request.headers.get("Access-Control-Request-Headers")
    response.headers["Access-Control-Allow-Headers"] = asked_for or "Content-Type"
    response.headers["Access-Control-Max-Age"] = "86400"
    response.headers["Cache-Control"] = "no-store"
    return response


def health_reply():
    """GET /health, for an uptime monitor: 200 when the database and the raw
    log both work, 503 otherwise. Not stored anywhere."""
    database_ok = False
    try:
        connection = get_connection()
        ensure_tables(connection)
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        connection.rollback()
        database_ok = True
    except Exception as error:
        log("Health check: database not reachable: " + describe(error))
        forget_connection()
    raw_log_ok = raw_log_writable()
    everything_ok = database_ok and raw_log_ok
    status = 200 if everything_ok else 503
    return json_reply(status, {"ok": everything_ok, "database": database_ok, "raw_log": raw_log_ok})


@app.before_request
def handle_any_request():
    """Flask calls this first for EVERY request, whatever its address or
    method (even PROPFIND or made-up ones). It always returns the reply
    itself, so Flask's own routing, which would answer 404 or 405 without
    the request ever being stored, never gets a turn."""
    if request.path == HEALTH_PATH and request.method in ("GET", "HEAD"):
        return health_reply()

    # Everything else is kept, even junk and wrong addresses: that is how
    # fake traffic gets noticed later.
    record = read_request()
    outcome = understand_request(record)
    saved_to_file = append_raw_log(record)
    saved_to_database = save_to_database(record, outcome, "live")

    status = outcome["status"]
    if status == 200 and record["path"] == BATCH_PATH:
        if not saved_to_file and not saved_to_database:
            # Kept nowhere: the game should keep the batch and try again.
            return json_reply(503, {"ok": False, "error": "could not save, try again later"})
        return json_reply(200, {
            "ok": True,
            "events": len(outcome["events"]),
            "saved_to_database": saved_to_database,
            "saved_to_file": saved_to_file,
        })
    if status == 400:
        # Stored anyway, but sending the same body again will not help.
        return json_reply(400, {"ok": False, "error": outcome["parse_error"]})
    if status == 204:
        return Response(status=204)
    if status == 200:
        return Response("Typeblasters log server is running.\n", status=200, mimetype="text/plain")
    return json_reply(404, {"ok": False, "error": "not found"})


def main():
    log("Typeblasters Log server starting")
    missing = []
    for name, value in (("DB_HOST", DB_HOST), ("DB_NAME", DB_NAME),
                        ("DB_USER", DB_USER), ("DB_PASSWORD", DB_PASSWORD)):
        if not value:
            missing.append(name)
    if missing:
        log("WARNING: not set in .env: " + ", ".join(missing))

    os.makedirs(RAW_LOG_FOLDER, exist_ok=True)
    try:
        ensure_tables(get_connection())
        log("Database OK: " + DB_NAME + " on " + DB_HOST + ":" + str(DB_PORT))
    except Exception as error:
        log("WARNING: database not reachable (" + describe(error) + "). "
            + "Requests are still saved to " + RAW_LOG_PATH)
    finally:
        forget_connection()  # the worker threads open their own

    log("Listening on " + LISTEN_HOST + ":" + str(LISTEN_PORT)
        + "   POST " + BATCH_PATH + "   GET " + HEALTH_PATH)
    serve(
        app,
        host=LISTEN_HOST,
        port=LISTEN_PORT,
        threads=WORKER_THREADS,
        # + 1 because waitress refuses a body of exactly this many bytes.
        max_request_body_size=MAX_BODY_BYTES + 1,
        # Waitress 3 deletes X-Forwarded-For and similar headers unless told
        # not to. Keep them: nginx and Cloudflare put the player's IP there.
        clear_untrusted_proxy_headers=False,
        # The port is reachable from the internet, so allow more open
        # connections than the default 100, so a few idle or slow clients
        # cannot take every slot. "poll" avoids select()'s limit of about
        # 1000 open files.
        connection_limit=CONNECTION_LIMIT,
        asyncore_use_poll=True,
    )


if __name__ == "__main__":
    main()
