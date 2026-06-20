import os, sys, asyncio, aiohttp, aiofiles, json, zlib, hashlib
import time, uuid, random, re, tempfile
import aiosqlite
import socket, ipaddress
from collections import OrderedDict
from datetime import datetime, timedelta
from urllib.parse import urlparse, quote_plus, unquote
from functools import wraps
from typing import List, Dict, Optional

try:
    import pypdf
except ImportError:
    pypdf = None

from dotenv import load_dotenv
load_dotenv()

from bs4 import BeautifulSoup
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from pyrogram.errors import FloodWait, MessageNotModified
import redis.asyncio as aioredis
import structlog

# ─────────────────────────────────────────────
#  LOGGING & NODE IDENTIFICATION
# ─────────────────────────────────────────────
NODE_ID = os.getenv("NODE_ID", uuid.uuid4().hex[:8])

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.PrintLoggerFactory()
)
log = structlog.get_logger().bind(node=NODE_ID)

# ─────────────────────────────────────────────
#  ENV CONFIG & PROXIES
# ─────────────────────────────────────────────
_RAW_API_ID = os.getenv("API_ID", "0")
API_ID    = int(_RAW_API_ID) if str(_RAW_API_ID).strip().isdigit() else 0
API_HASH  = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/pharma_bot")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.strip().isdigit()]
ALLOWED_USERS = [int(x) for x in os.getenv("ALLOWED_USERS", "").split(",") if x.strip().isdigit()]
PROXIES = [p.strip() for p in os.getenv("PROXIES", "").split(",") if p.strip()]  # BUG FIX 1: strip whitespace from proxy strings

GOOGLE_SEARCH_API_KEY = os.getenv("GOOGLE_SEARCH_API_KEY", "")
GOOGLE_SEARCH_CX = os.getenv("GOOGLE_SEARCH_CX", "")

# ─────────────────────────────────────────────
#  ASYNCIO EVENT LOOP — MUST BE CREATED FIRST
#  BUG FIX 2: Semaphores, Locks, Queues MUST be
#  created inside an async context OR after loop
#  is running. Creating them at module-level with
#  Python 3.10+ raises DeprecationWarning and can
#  bind to the wrong loop. We defer init to startup.
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
#  PROXY TRACKING STATE
# ─────────────────────────────────────────────
PROXY_FAILS: Dict[str, int] = {p: 0 for p in PROXIES}
PROXY_SUCCESS: Dict[str, int] = {p: 0 for p in PROXIES}
PROXY_LATENCY: Dict[str, float] = {p: 5.0 for p in PROXIES}
DEAD_PROXIES: set = set()
PROXY_REVIVE_COUNTS: Dict[str, int] = {p: 0 for p in PROXIES}
ABSOLUTE_DEAD_PROXIES: set = set()
PROXY_BLACKLIST_THRESHOLD = 10

DOMAIN_PROXY_STATS: Dict[str, Dict] = {
    "google":   {p: {"fails": 0, "success": 0, "latency": 5.0} for p in PROXIES},
    "bing":     {p: {"fails": 0, "success": 0, "latency": 5.0} for p in PROXIES},
    "ddg":      {p: {"fails": 0, "success": 0, "latency": 5.0} for p in PROXIES},
    "telegram": {p: {"fails": 0, "success": 0, "latency": 5.0} for p in PROXIES},
}

# ─────────────────────────────────────────────
#  CONSTANTS
# ─────────────────────────────────────────────
YEARS = list(range(2024, 2014, -1))

EXAM_TREE = {
    "AIIMS_CRE": {
        "label": "🏥 AIIMS CRE Pharmacist",
        "regions": [
            "AIIMS Delhi", "AIIMS Bhopal", "AIIMS Raipur", "AIIMS Rishikesh",
            "AIIMS Jodhpur", "AIIMS Patna", "AIIMS Bhubaneswar", "AIIMS Nagpur",
            "AIIMS Kalyani", "AIIMS Mangalagiri", "AIIMS Bibinagar",
            "AIIMS Gorakhpur", "AIIMS Rajkot", "AIIMS Deoghar",
        ],
    },
    "ESIC": {
        "label": "🏨 ESIC Pharmacist",
        "regions": [
            "ESIC Central", "ESIC Delhi", "ESIC Mumbai", "ESIC Chennai",
            "ESIC Kolkata", "ESIC Hyderabad", "ESIC Bengaluru",
        ],
    },
    "DSSSB": {
        "label": "🏛️ DSSSB Pharmacist",
        "regions": ["DSSSB Delhi"],
    },
    "RUHS": {
        "label": "🎓 RUHS Pharmacist",
        "regions": ["RUHS Rajasthan"],
    },
    "NHM": {
        "label": "🌿 NHM Pharmacist",
        "regions": [
            "NHM UP", "NHM MP", "NHM Bihar", "NHM Rajasthan",
            "NHM Maharashtra", "NHM Gujarat", "NHM Punjab",
        ],
    },
    "DRUG_INSPECTOR": {
        "label": "💊 Drug Inspector",
        "regions": [
            "Drug Inspector Central", "Drug Inspector UP", "Drug Inspector MP",
            "Drug Inspector Bihar", "Drug Inspector Rajasthan",
            "Drug Inspector Delhi", "Drug Inspector Maharashtra", "Drug Inspector Gujarat",
        ],
    },
    "GPAT": {
        "label": "📚 GPAT / NIPER",
        "regions": ["GPAT All India"],
    },
    "STATE_PHARMA": {
        "label": "🗺️ State PSC Pharmacist",
        "regions": [
            "UPPSC Pharmacist", "MPPSC Pharmacist", "BPSC Pharmacist",
            "RPSC Pharmacist", "HPSC Pharmacist", "KPSC Pharmacist",
            "TNPSC Pharmacist", "WBPSC Pharmacist", "SSC Pharmacist",
        ],
    },
}

MATERIAL_TYPES = {
    "pyq":      "📝 Previous Year Papers",
    "syllabus": "📋 Syllabus",
    "notes":    "📖 Study Notes / Material",
    "anskey":   "✅ Answer Keys",
    "mock":     "🎯 Mock Tests",
    "books":    "📗 Reference Books PDF",
}

TELEGRAM_CHANNELS = [
    "pharmacist_pyq", "pharma_exam_material", "aiims_pharmacist_pyq",
    "drug_inspector_notes", "pharmacy_exam_pdf", "gpat_material",
    "pharmacist_exam_zone", "pharma_study_hub",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

# BUG FIX 3: REGION_ID_MAP size guard was using wrong eviction —
# `next(iter(OrderedDict))` on a plain dict has no order guarantee in older
# Pythons. Use OrderedDict explicitly or popitem(last=False).
REGION_ID_MAP: OrderedDict = OrderedDict()

def get_region_hash(region_str: str) -> str:
    rh = hashlib.md5(region_str.encode()).hexdigest()[:8]
    REGION_ID_MAP[rh] = region_str
    if len(REGION_ID_MAP) > 1000:
        REGION_ID_MAP.popitem(last=False)  # BUG FIX 3: guaranteed FIFO eviction
    return rh

# ─────────────────────────────────────────────
#  GLOBAL STATE — primitives only at module level
#  BUG FIX 2 (continued): asyncio primitives are
#  created inside init_async_state() below.
# ─────────────────────────────────────────────
SESSION:    Optional[aiohttp.ClientSession] = None
REDIS:      Optional[aioredis.Redis]        = None
MONGO_DB                                    = None

# Declared here, assigned in init_async_state()
STATE_LOCK:        asyncio.Lock
SQLITE_LOCK:       asyncio.Lock
DNS_CACHE_LOCK:    asyncio.Lock
CACHE_LOCK:        asyncio.Lock
ACTIVE_DL_LOCK:    asyncio.Lock
DOWNLOAD_QUEUE:    asyncio.Queue
DOWNLOAD_SEMAPHORE: asyncio.Semaphore
SCRAPE_SEMAPHORE:   asyncio.Semaphore
DOMAIN_LIMITS:      Dict[str, asyncio.Semaphore]

DNS_CACHE: Dict[str, Dict] = {}
DNS_CACHE_TTL = 3600
USER_DL_TIMESTAMPS: Dict[int, float] = {}

DOMAIN_PENALTY = {"google": 0.0, "bing": 0.0, "ddg": 0.0, "telegram": 0.0}
DOMAIN_CIRCUIT_BREAKER = {"google": 0, "bing": 0, "ddg": 0, "telegram": 0}
CIRCUIT_TRIP_TIME = {"google": 0.0, "bing": 0.0, "ddg": 0.0, "telegram": 0.0}
CIRCUIT_STATE = {"google": "CLOSED", "bing": "CLOSED", "ddg": "CLOSED", "telegram": "CLOSED"}

ACTIVE_DL      = 0
BURST_WORKERS  = 0
MAX_BURST_WORKERS = 10
MAX_TOTAL_WORKERS = 15
BURST_TASKS: set = set()

LAST_BURST_TIME = 0.0
BURST_COOLDOWN = 30.0

LOCAL_MEM_CACHE: Dict[str, Dict] = {}

TASK_REGISTRY: Dict[str, set] = {
    "static_workers": set(),
    "burst_workers":  set(),
    "background_loops": set()
}

BOT_METRICS: Dict = {
    "total_searches": 0,
    "pdfs_downloaded": 0,
    "bytes_downloaded": 0,
    "api_fallback_hits": 0,
    "engine_errors": {
        "google": 0, "bing": 0, "ddg": 0, "telegram": 0
    }
}
MONGO_SYNCED_SIDS: set = set()

DOWNLOAD_LATENCY_EMA = 0.0
EVENT_LOOP_LAG       = 0.0

QUEUE_PAUSED   = False
BOT_START_TIME = time.time()
LAST_BAN_CLEAR_TIME = time.time()

SESSION_MAP: OrderedDict = OrderedDict()
SESSION_EXPIRY: Dict[str, float] = {}
SESSION_LAST_ACCESSED: Dict[str, float] = {}
SESSION_TTL = 3600
REDIS_DISABLED_WARNING = False

GLOBAL_BANS: set = set()
USER_VIOLATIONS: Dict[int, int] = {}
USER_DL_COUNTS: Dict[int, int] = {}
USER_DAILY_QUOTA: Dict[int, int] = {}

# ─────────────────────────────────────────────
#  ASYNC STATE INITIALIZER
#  BUG FIX 2: All asyncio primitives created here,
#  called from main() AFTER the event loop starts.
# ─────────────────────────────────────────────
async def init_async_state():
    global STATE_LOCK, SQLITE_LOCK, DNS_CACHE_LOCK, CACHE_LOCK, ACTIVE_DL_LOCK
    global DOWNLOAD_QUEUE, DOWNLOAD_SEMAPHORE, SCRAPE_SEMAPHORE, DOMAIN_LIMITS

    STATE_LOCK      = asyncio.Lock()
    SQLITE_LOCK     = asyncio.Lock()
    DNS_CACHE_LOCK  = asyncio.Lock()
    CACHE_LOCK      = asyncio.Lock()
    ACTIVE_DL_LOCK  = asyncio.Lock()

    DOWNLOAD_QUEUE      = asyncio.Queue(maxsize=1000)
    DOWNLOAD_SEMAPHORE  = asyncio.Semaphore(5)
    SCRAPE_SEMAPHORE    = asyncio.Semaphore(8)
    DOMAIN_LIMITS       = {
        "google":   asyncio.Semaphore(2),
        "bing":     asyncio.Semaphore(3),
        "ddg":      asyncio.Semaphore(4),
        "telegram": asyncio.Semaphore(3),
    }
    log.info("async_state_initialized")

# ─────────────────────────────────────────────
#  CIRCUIT BREAKER
# ─────────────────────────────────────────────
def is_circuit_open(domain: str) -> bool:
    if CIRCUIT_STATE[domain] == "OPEN":
        if time.time() > CIRCUIT_TRIP_TIME.get(domain, 0):
            CIRCUIT_STATE[domain] = "HALF_OPEN"
            log.info("circuit_half_open_testing_recovery", domain=domain)
            return False
        return True
    return False

def trip_circuit(domain: str):
    DOMAIN_CIRCUIT_BREAKER[domain] += 1
    if DOMAIN_CIRCUIT_BREAKER[domain] > 5:
        CIRCUIT_STATE[domain] = "OPEN"
        CIRCUIT_TRIP_TIME[domain] = time.time() + 300
        log.error("circuit_breaker_tripped_open", domain=domain, penalty_seconds=300)
        DOMAIN_CIRCUIT_BREAKER[domain] = 0

def heal_circuit(domain: str):
    DOMAIN_CIRCUIT_BREAKER[domain] = 0
    if CIRCUIT_STATE[domain] in ["OPEN", "HALF_OPEN"]:
        CIRCUIT_STATE[domain] = "CLOSED"
        log.info("circuit_fully_healed_closed", domain=domain)

# ─────────────────────────────────────────────
#  SQLITE SUBSYSTEM
# ─────────────────────────────────────────────
SQLITE_DB_PATH = "sessions_spillover.db"

async def init_sqlite_spillover():
    async with SQLITE_LOCK:
        try:
            async with aiosqlite.connect(SQLITE_DB_PATH) as db:
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS spillover
                    (sid TEXT PRIMARY KEY, data BLOB, ts REAL)
                """)
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS persistent_metrics
                    (id TEXT PRIMARY KEY, data TEXT)
                """)
                # BUG FIX 4: Enable WAL mode for concurrent read/write
                await db.execute("PRAGMA journal_mode=WAL")
                await db.commit()
        except Exception as e:
            log.error("sqlite_init_fail", err=str(e))

async def sqlite_save(sid: str, data: bytes):
    async with SQLITE_LOCK:
        try:
            async with aiosqlite.connect(SQLITE_DB_PATH) as db:
                await db.execute(
                    "REPLACE INTO spillover (sid, data, ts) VALUES (?, ?, ?)",
                    (sid, data, time.time())
                )
                await db.commit()
        except Exception as e:
            log.error("sqlite_spillover_save_fail", err=str(e)[:60])

async def sqlite_load(sid: str) -> Optional[bytes]:
    async with SQLITE_LOCK:
        try:
            async with aiosqlite.connect(SQLITE_DB_PATH) as db:
                async with db.execute(
                    "SELECT data FROM spillover WHERE sid=?", (sid,)
                ) as cursor:
                    row = await cursor.fetchone()
                    # BUG FIX 5: row[0] might be memoryview from aiosqlite BLOB;
                    # must convert to bytes explicitly.
                    return bytes(row[0]) if row else None
        except Exception:
            return None

async def save_persistent_metrics():
    payload = {
        "BOT_METRICS":     BOT_METRICS,
        "USER_DAILY_QUOTA": USER_DAILY_QUOTA,
        "USER_DL_COUNTS":  USER_DL_COUNTS,
    }
    async with SQLITE_LOCK:
        try:
            async with aiosqlite.connect(SQLITE_DB_PATH) as db:
                await db.execute(
                    "REPLACE INTO persistent_metrics (id, data) VALUES (?, ?)",
                    ("bot_metrics_v2", json.dumps(payload))
                )
                await db.commit()
        except Exception as e:
            log.error("save_persistent_metrics_fail", err=str(e))  # BUG FIX 6: was silently passing

async def load_persistent_metrics():
    async with SQLITE_LOCK:
        try:
            async with aiosqlite.connect(SQLITE_DB_PATH) as db:
                async with db.execute(
                    "SELECT data FROM persistent_metrics WHERE id=?",
                    ("bot_metrics_v2",)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        loaded = json.loads(row[0])
                        BOT_METRICS.update(loaded.get("BOT_METRICS", {}))
                        USER_DAILY_QUOTA.update(loaded.get("USER_DAILY_QUOTA", {}))
                        USER_DL_COUNTS.update(loaded.get("USER_DL_COUNTS", {}))
        except Exception as e:
            log.error("load_persistent_metrics_fail", err=str(e))  # BUG FIX 6: was silently passing

# ─────────────────────────────────────────────
#  TASK ORCHESTRATION & QUEUE PERSISTENCE
# ─────────────────────────────────────────────
def log_task_exception(task: asyncio.Task):
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error("background_task_crashed_silently", task_name=task.get_name(), err=str(e))

def create_safe_task(coro, name: str) -> asyncio.Task:
    task = asyncio.create_task(coro, name=name)
    task.add_done_callback(log_task_exception)
    return task

def dump_local_queue():
    items = []
    while not DOWNLOAD_QUEUE.empty():
        try:
            item = DOWNLOAD_QUEUE.get_nowait()
            # BUG FIX 7: original checked len==5 but queue items may have
            # variable structure. Check minimum required fields safely.
            if isinstance(item, tuple) and len(item) >= 5:
                items.append({
                    "chat_id": item[1],
                    "url":     item[2],
                    "title":   item[3],
                    "user_id": item[4],
                })
            DOWNLOAD_QUEUE.task_done()
        except asyncio.QueueEmpty:
            break
        except Exception:
            pass
    if items:
        try:
            with open("local_queue_backup.json", "w") as f:
                json.dump(items, f)
            log.info("local_queue_saved_to_disk", count=len(items))
        except Exception as e:
            log.error("dump_local_queue_fail", err=str(e))

def load_local_queue():
    if os.path.exists("local_queue_backup.json"):
        try:
            with open("local_queue_backup.json", "r") as f:
                items = json.load(f)
            loaded = 0
            for item in items:
                try:
                    DOWNLOAD_QUEUE.put_nowait((
                        "restored_app",
                        item["chat_id"],
                        item["url"],
                        item["title"],
                        item["user_id"],
                    ))
                    loaded += 1
                except asyncio.QueueFull:
                    log.warning(
                        "local_queue_full_during_restore",
                        loaded=loaded,
                        dropped=len(items) - loaded,  # BUG FIX 8: was wrong count
                    )
                    break
            os.remove("local_queue_backup.json")
            log.info("local_queue_restored_from_disk", count=loaded)
        except Exception as e:
            log.error("load_local_queue_fail", err=str(e))

# ─────────────────────────────────────────────
#  PYROGRAM CLIENT
# ─────────────────────────────────────────────
app = Client(
    "pharma_ultimate_v2",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

# ─────────────────────────────────────────────
#  SAFE HANDLER DECORATOR
#  BUG FIX 9: `tid` was created but never used in
#  the FloodWait retry — error trace lost on retry.
#  Also: unauthorized check must handle None from_user
#  (channel posts have no from_user).
# ─────────────────────────────────────────────
def safe(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        user_id = None
        for a in args:
            if isinstance(a, Message):
                # BUG FIX 9a: from_user is None for channel/anonymous messages
                user_id = a.from_user.id if a.from_user else None
                break
            elif isinstance(a, CallbackQuery):
                user_id = a.from_user.id if a.from_user else None
                break

        if ALLOWED_USERS and user_id and user_id not in ALLOWED_USERS and user_id not in ADMIN_IDS:
            log.warning("unauthorized_access_attempt", user_id=user_id)
            return

        tid = uuid.uuid4().hex[:8]
        try:
            return await func(*args, **kwargs)
        except FloodWait as fw:
            log.warning("flood_wait", fn=func.__name__, seconds=fw.value, tid=tid)
            await asyncio.sleep(fw.value + 1)
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                log.error("handler_crash_after_flood_retry", fn=func.__name__, err=str(e), tid=tid)
        except MessageNotModified:
            pass
        except Exception as e:
            log.error("handler_crash", fn=func.__name__, err=str(e), tid=tid)
            for a in args:
                if isinstance(a, Message):
                    try:
                        await a.reply_text(f"❌ Error. Trace: `{tid}`")
                    except Exception:
                        pass
                    break
                elif isinstance(a, CallbackQuery):
                    try:
                        await a.answer(f"⚠️ Error. Trace: {tid}", show_alert=True)
                    except Exception:
                        pass
                    break
    return wrapper

# ─────────────────────────────────────────────
#  REDIS HELPERS & GLOBAL BANS
#  BUG FIX 10: is_rate_limited used STATE_LOCK
#  then immediately called REDIS (which itself
#  may acquire locks). Holding STATE_LOCK across
#  an await on REDIS is a deadlock risk.
#  Fixed by releasing STATE_LOCK before Redis ops.
# ─────────────────────────────────────────────
async def is_rate_limited(user_id: int, prefix: str, ttl: int = 5) -> bool:
    # Check ban first — short critical section
    async with STATE_LOCK:
        is_banned = user_id in GLOBAL_BANS

    if is_banned:
        return True

    if not REDIS:
        return False

    key = f"rl:{prefix}:{user_id}"
    try:
        hit = await REDIS.get(key)
    except Exception:
        return False  # BUG FIX 10a: Redis error → don't block user

    if hit:
        async with STATE_LOCK:  # BUG FIX 10: STATE_LOCK NOT held across await
            USER_VIOLATIONS[user_id] = USER_VIOLATIONS.get(user_id, 0) + 1
            if USER_VIOLATIONS[user_id] > 15:
                GLOBAL_BANS.add(user_id)
                log.warning("global_ban_applied_to_spammer", user_id=user_id)
        return True

    try:
        await REDIS.setex(key, ttl, b"1")
    except Exception:
        pass
    return False

async def redis_get(key: str) -> Optional[bytes]:
    if not REDIS:
        return None
    try:
        val = await REDIS.get(key)
        return bytes(val) if val is not None else None  # BUG FIX 11: ensure bytes, not memoryview
    except Exception:
        return None

async def redis_set(key: str, value: bytes, ttl: int = 7200):
    if not REDIS:
        return
    try:
        await REDIS.setex(key, ttl, value)
    except Exception as e:
        log.warning("redis_set_fail", key=key, err=str(e))  # BUG FIX 12: was silently passing

# ─────────────────────────────────────────────
#  COMPRESSION HELPERS
#  BUG FIX 13: decompress() returned list but
#  callers may pass data that serialized as dict.
#  Return Any and let callers cast.
# ─────────────────────────────────────────────
def compress(data) -> bytes:
    return zlib.compress(json.dumps(data, ensure_ascii=False).encode("utf-8"), level=6)

def decompress(b: bytes):
    return json.loads(zlib.decompress(b).decode("utf-8"))

def normalize_bytes(x) -> Optional[bytes]:
    if x is None:
        return None
    return bytes(x)

# ─────────────────────────────────────────────
#  SEARCH ID & CACHE KEYS
# ─────────────────────────────────────────────
def make_search_id(exam_key: str, region: str, year: int, mat: str) -> str:
    raw = f"{exam_key}|{region}|{year}|{mat}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]

def make_search_id_v2(exam_key: str, region: str, year: int, mat: str, user_id=None) -> str:
    raw = f"{exam_key}|{region}|{year}|{mat}|{user_id or 0}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]

# ─────────────────────────────────────────────
#  SSRF-SAFE DNS RESOLVER
# ─────────────────────────────────────────────
_PRIVATE_PREFIXES = (
    "10.", "172.16.", "172.17.", "172.18.", "172.19.", "172.20.",
    "172.21.", "172.22.", "172.23.", "172.24.", "172.25.", "172.26.",
    "172.27.", "172.28.", "172.29.", "172.30.", "172.31.", "192.168.",
)
_PRIVATE_EXACT = {"127.0.0.1", "0.0.0.0", "::1", "localhost"}

def _host_is_safe(host: str) -> bool:
    h = host.lower().split(":")[0].strip("[]")
    if h in _PRIVATE_EXACT:
        return False
    if any(h.startswith(p) for p in _PRIVATE_PREFIXES):
        return False
    return True

async def async_valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return False

        clean_host = (p.hostname or "").lower().strip("[]")
        if not clean_host:
            return False

        # BUG FIX 14: original called safe_host(p.hostname) which returns bool,
        # then called safe_host again inside. Consolidated here.
        if not _host_is_safe(clean_host):
            return False

        async with DNS_CACHE_LOCK:
            cached = DNS_CACHE.get(clean_host)
            if cached and time.time() - cached["ts"] < DNS_CACHE_TTL:
                return cached["safe"]

        try:
            addr_info = await asyncio.wait_for(
                asyncio.to_thread(socket.getaddrinfo, clean_host, None),
                timeout=5.0  # BUG FIX 15: DNS lookup had no timeout — could block forever
            )
        except asyncio.TimeoutError:
            log.warning("dns_lookup_timeout", host=clean_host)
            return False

        is_safe = all(
            not (ipaddress.ip_address(res[4][0]).is_private or
                 ipaddress.ip_address(res[4][0]).is_loopback)
            for res in addr_info
        )

        async with DNS_CACHE_LOCK:
            DNS_CACHE[clean_host] = {"safe": is_safe, "ts": time.time()}

        return is_safe
    except Exception:
        return False

def safe_host(host: str) -> bool:
    clean_host = (host or "").split(":")[0].strip("[]")
    return _host_is_safe(clean_host)

def valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and safe_host(p.hostname or "")
    except Exception:
        return False

class SSRFSenseResolver(aiohttp.ThreadedResolver):
    async def resolve(self, host, port=0, family=socket.AF_INET):
        ips = await super().resolve(host, port, family)
        for ip in ips:
            ip_obj = ipaddress.ip_address(ip["host"])
            if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local:
                raise ValueError(f"SSRF Blocked at DNS level: {host} -> {ip['host']}")
        return ips

# ─────────────────────────────────────────────
#  PROXY SELECTOR
# ─────────────────────────────────────────────
async def get_random_proxy() -> Optional[str]:
    async with STATE_LOCK:
        if not PROXIES:
            return None
        valid_proxies = [
            p for p in PROXIES
            if p not in DEAD_PROXIES
            and p not in ABSOLUTE_DEAD_PROXIES
            and PROXY_FAILS.get(p, 0) < PROXY_BLACKLIST_THRESHOLD
        ]
        if not valid_proxies:
            log.warning("all_proxies_dead_or_failed", dead=len(DEAD_PROXIES))
            # BUG FIX 16: original code cut off here with no return →
            # function fell through returning None implicitly (OK) but
            # the condition `< PROXY_BLACKLIST_THRESHOLD` was missing from
            # the truncated original. Restored properly.
            return None

        # Weighted selection: lower latency = higher weight
        weights = [
            1.0 / max(PROXY_LATENCY.get(p, 5.0), 0.1)
            for p in valid_proxies
        ]
        total = sum(weights)
        probs = [w / total for w in weights]
        return random.choices(valid_proxies, weights=probs, k=1)[0]

async def mark_proxy_success(proxy: str, latency: float):
    async with STATE_LOCK:
        PROXY_SUCCESS[proxy] = PROXY_SUCCESS.get(proxy, 0) + 1
        PROXY_FAILS[proxy]   = max(0, PROXY_FAILS.get(proxy, 0) - 1)
        # Exponential moving average for latency
        old = PROXY_LATENCY.get(proxy, latency)
        PROXY_LATENCY[proxy] = 0.8 * old + 0.2 * latency
        if proxy in DEAD_PROXIES:
            DEAD_PROXIES.discard(proxy)
            PROXY_REVIVE_COUNTS[proxy] = PROXY_REVIVE_COUNTS.get(proxy, 0) + 1
            log.info("proxy_revived", proxy=proxy)

async def mark_proxy_fail(proxy: str):
    async with STATE_LOCK:
        PROXY_FAILS[proxy] = PROXY_FAILS.get(proxy, 0) + 1
        if PROXY_FAILS[proxy] >= PROXY_BLACKLIST_THRESHOLD:
            DEAD_PROXIES.add(proxy)
            log.warning("proxy_marked_dead", proxy=proxy, fails=PROXY_FAILS[proxy])
        if PROXY_FAILS[proxy] >= PROXY_BLACKLIST_THRESHOLD * 3:
            ABSOLUTE_DEAD_PROXIES.add(proxy)
            log.error("proxy_permanently_blacklisted", proxy=proxy)

# ─────────────────────────────────────────────
#  SESSION / CONNECTION POOL INIT
#  BUG FIX 17: aiohttp.ClientSession must be
#  created inside a running event loop.
# ─────────────────────────────────────────────
async def init_session():
    global SESSION
    connector = aiohttp.TCPConnector(
        resolver=SSRFSenseResolver(),
        ssl=True,
        limit=100,
        limit_per_host=10,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    SESSION = aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=30, connect=10, sock_read=20),
        headers={"User-Agent": random.choice(USER_AGENTS)},
    )
    log.info("aiohttp_session_created")

async def close_session():
    global SESSION
    if SESSION and not SESSION.closed:
        await SESSION.close()
        SESSION = None
    log.info("aiohttp_session_closed")

# ─────────────────────────────────────────────
#  REDIS INIT
# ─────────────────────────────────────────────
async def init_redis():
    global REDIS, REDIS_DISABLED_WARNING
    try:
        REDIS = await aioredis.from_url(
            REDIS_URL,
            encoding="utf-8",
            decode_responses=False,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        await REDIS.ping()
        log.info("redis_connected", url=REDIS_URL)
    except Exception as e:
        log.warning("redis_unavailable_falling_back_to_local", err=str(e))
        REDIS = None
        REDIS_DISABLED_WARNING = True

async def close_redis():
    global REDIS
    if REDIS:
        await REDIS.close()
        REDIS = None

# ─────────────────────────────────────────────
#  MONGO INIT
# ─────────────────────────────────────────────
async def init_mongo():
    global MONGO_DB
    try:
        client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        await client.admin.command("ping")
        db_name = urlparse(MONGO_URI).path.lstrip("/") or "pharma_bot"
        MONGO_DB = client[db_name]
        log.info("mongo_connected", db=db_name)
    except Exception as e:
        log.warning("mongo_unavailable", err=str(e))
        MONGO_DB = None

# ─────────────────────────────────────────────
#  STARTUP / SHUTDOWN
# ─────────────────────────────────────────────
async def on_startup():
    await init_async_state()       # BUG FIX 2: must be first
    await init_sqlite_spillover()
    await load_persistent_metrics()
    await init_session()
    await init_redis()
    await init_mongo()
    load_local_queue()
    log.info("bot_startup_complete", node=NODE_ID)

async def on_shutdown():
    dump_local_queue()
    await save_persistent_metrics()
    await close_session()
    await close_redis()
    log.info("bot_shutdown_complete", node=NODE_ID)

# ─────────────────────────────────────────────
#  MAIN ENTRY POINT
# ─────────────────────────────────────────────
async def main():
    await on_startup()
    try:
        await app.start()
        log.info("pyrogram_bot_started")
        await asyncio.Event().wait()   # run forever
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        try:
            await app.stop()
        except Exception:
            pass
        await on_shutdown()

if __name__ == "__main__":
    asyncio.run(main())