import os, sys, asyncio, aiohttp, aiofiles, json, zlib, hashlib
import time, uuid, random, re, tempfile
import aiosqlite
import socket, ipaddress
from collections import OrderedDict
from datetime import datetime, timedelta
from urllib.parse import urlparse, quote_plus, unquote
from functools import wraps
from typing import List, Dict, Optional, Any, Tuple

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
#  LOGGING
# ─────────────────────────────────────────────
NODE_ID = os.getenv("NODE_ID", uuid.uuid4().hex[:8])

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.PrintLoggerFactory(),
)
log = structlog.get_logger().bind(node=NODE_ID)

# ─────────────────────────────────────────────
#  ENV CONFIG
# ─────────────────────────────────────────────
_RAW_API_ID = os.getenv("API_ID", "0")
API_ID    = int(_RAW_API_ID) if _RAW_API_ID.strip().isdigit() else 0
API_HASH  = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/pharma_bot")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.strip().isdigit()]
ALLOWED_USERS = [int(x) for x in os.getenv("ALLOWED_USERS", "").split(",") if x.strip().isdigit()]
PROXIES = [p.strip() for p in os.getenv("PROXIES", "").split(",") if p.strip()]

GOOGLE_SEARCH_API_KEY = os.getenv("GOOGLE_SEARCH_API_KEY", "")
GOOGLE_SEARCH_CX      = os.getenv("GOOGLE_SEARCH_CX", "")

# ─────────────────────────────────────────────
#  USER AGENTS
# ─────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

def random_ua() -> str:
    """Always returns a fresh random UA — called per-request, not at startup."""
    return random.choice(USER_AGENTS)

# ─────────────────────────────────────────────
#  SSRF-SAFE DNS RESOLVER — FIX #1
#
#  Original bug: subclassed aiohttp.ThreadedResolver
#  and made resolve() async — ThreadedResolver is
#  internally sync and aiohttp does NOT call async
#  resolve(). This caused silent DNS bypass or crash.
#
#  Fix: implement aiohttp.AbstractResolver properly.
#  resolve() is async in AbstractResolver and we do
#  the blocking getaddrinfo in a thread via
#  asyncio.to_thread() with a hard timeout.
# ─────────────────────────────────────────────
_PRIVATE_PREFIXES = (
    "10.", "172.16.", "172.17.", "172.18.", "172.19.", "172.20.",
    "172.21.", "172.22.", "172.23.", "172.24.", "172.25.", "172.26.",
    "172.27.", "172.28.", "172.29.", "172.30.", "172.31.", "192.168.",
)
_PRIVATE_EXACT = {"127.0.0.1", "0.0.0.0", "::1", "localhost"}

def _ip_is_safe(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
        return not (ip.is_private or ip.is_loopback or ip.is_link_local
                    or ip.is_multicast or ip.is_unspecified)
    except ValueError:
        return False

def _host_is_safe(host: str) -> bool:
    h = host.lower().split(":")[0].strip("[]")
    if h in _PRIVATE_EXACT:
        return False
    if any(h.startswith(p) for p in _PRIVATE_PREFIXES):
        return False
    return True

class SSRFSafeResolver(aiohttp.AbstractResolver):
    """
    Correct implementation of aiohttp.AbstractResolver.
    - async resolve() as required by aiohttp internals
    - DNS lookup in thread with timeout to prevent hang
    - Blocks private/loopback IPs at resolution time
    """

    async def resolve(
        self, host: str, port: int = 0, family: int = socket.AF_INET
    ) -> List[Dict]:
        if not _host_is_safe(host):
            raise aiohttp.ClientConnectorError(
                connection_key=None,  # type: ignore[arg-type]
                os_error=OSError(f"SSRF: blocked host {host!r}"),
            )
        try:
            infos = await asyncio.wait_for(
                asyncio.to_thread(
                    socket.getaddrinfo, host, port,
                    family, socket.SOCK_STREAM
                ),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            raise aiohttp.ClientConnectorError(
                connection_key=None,  # type: ignore[arg-type]
                os_error=OSError(f"DNS timeout for {host!r}"),
            )

        results = []
        for af, kind, proto, canonname, sockaddr in infos:
            ip = sockaddr[0]
            if not _ip_is_safe(ip):
                raise aiohttp.ClientConnectorError(
                    connection_key=None,  # type: ignore[arg-type]
                    os_error=OSError(f"SSRF: {host!r} resolved to private IP {ip}"),
                )
            results.append({
                "hostname": host,
                "host":     ip,
                "port":     sockaddr[1],
                "family":   af,
                "proto":    proto,
                "flags":    0,
            })
        if not results:
            raise aiohttp.ClientConnectorError(
                connection_key=None,  # type: ignore[arg-type]
                os_error=OSError(f"No valid addresses for {host!r}"),
            )
        return results

    async def close(self) -> None:
        pass  # nothing to clean up

# ─────────────────────────────────────────────
#  URL VALIDATORS
# ─────────────────────────────────────────────
def valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and _host_is_safe(p.hostname or "")
    except Exception:
        return False

async def async_valid_url(url: str, dns_cache: Dict, dns_cache_lock: asyncio.Lock) -> bool:
    """
    Async URL validator with DNS-level SSRF check and caching.
    dns_cache + lock passed in (not global) to avoid import-time init issues.
    """
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return False
        host = (p.hostname or "").lower().strip("[]")
        if not host or not _host_is_safe(host):
            return False

        async with dns_cache_lock:
            cached = dns_cache.get(host)
            if cached and time.time() - cached["ts"] < 3600:
                return cached["safe"]

        try:
            infos = await asyncio.wait_for(
                asyncio.to_thread(socket.getaddrinfo, host, None),
                timeout=5.0,
            )
            safe = all(_ip_is_safe(r[4][0]) for r in infos)
        except asyncio.TimeoutError:
            safe = False

        async with dns_cache_lock:
            dns_cache[host] = {"safe": safe, "ts": time.time()}
        return safe
    except Exception:
        return False

# ─────────────────────────────────────────────
#  PROXY POOL MANAGER — FIX #2, #7
# ─────────────────────────────────────────────
PROXY_BLACKLIST_THRESHOLD = 10

class ProxyPool:
    """
    Thread-safe proxy pool with latency-weighted selection.
    All edge cases handled:
      - empty pool → None
      - all proxies dead → None
      - zero-weight edge case → uniform fallback
    """

    def __init__(self, proxies: List[str]):
        self._lock    = None   # set in async_init()
        self._proxies = list(proxies)
        self._fails:    Dict[str, int]   = {p: 0   for p in proxies}
        self._success:  Dict[str, int]   = {p: 0   for p in proxies}
        self._latency:  Dict[str, float] = {p: 5.0 for p in proxies}
        self._dead:     set = set()
        self._abs_dead: set = set()
        self._revives:  Dict[str, int] = {p: 0 for p in proxies}

    async def async_init(self):
        self._lock = asyncio.Lock()

    def _valid(self) -> List[str]:
        return [
            p for p in self._proxies
            if p not in self._dead
            and p not in self._abs_dead
            and self._fails.get(p, 0) < PROXY_BLACKLIST_THRESHOLD
        ]

    async def get(self) -> Optional[str]:
        if not self._proxies:
            return None
        async with self._lock:
            valid = self._valid()
            if not valid:
                log.warning("proxy_pool_exhausted", dead=len(self._dead))
                return None
            # FIX #2: zero-division guard + uniform fallback
            weights = [1.0 / max(self._latency.get(p, 5.0), 0.01) for p in valid]
            total = sum(weights)
            if total <= 0:
                return random.choice(valid)   # uniform fallback
            probs = [w / total for w in weights]
            return random.choices(valid, weights=probs, k=1)[0]

    async def success(self, proxy: str, latency: float):
        async with self._lock:
            self._success[proxy] = self._success.get(proxy, 0) + 1
            self._fails[proxy]   = max(0, self._fails.get(proxy, 0) - 1)
            old = self._latency.get(proxy, latency)
            self._latency[proxy] = 0.8 * old + 0.2 * latency   # EMA
            if proxy in self._dead:
                self._dead.discard(proxy)
                self._revives[proxy] = self._revives.get(proxy, 0) + 1
                log.info("proxy_revived", proxy=proxy)

    async def fail(self, proxy: str):
        async with self._lock:
            self._fails[proxy] = self._fails.get(proxy, 0) + 1
            f = self._fails[proxy]
            if f >= PROXY_BLACKLIST_THRESHOLD:
                self._dead.add(proxy)
                log.warning("proxy_marked_dead", proxy=proxy, fails=f)
            if f >= PROXY_BLACKLIST_THRESHOLD * 3:
                self._abs_dead.add(proxy)
                log.error("proxy_permanently_blacklisted", proxy=proxy)

    def stats(self) -> Dict:
        return {
            "total":    len(self._proxies),
            "dead":     len(self._dead),
            "abs_dead": len(self._abs_dead),
            "active":   len(self._valid()),
        }

# ─────────────────────────────────────────────
#  CIRCUIT BREAKER
# ─────────────────────────────────────────────
class CircuitBreaker:
    TRIP_THRESHOLD = 5
    RESET_AFTER    = 300  # seconds

    def __init__(self, domains: List[str]):
        self._fails: Dict[str, int]   = {d: 0   for d in domains}
        self._state: Dict[str, str]   = {d: "CLOSED" for d in domains}
        self._trip_at: Dict[str, float] = {d: 0.0 for d in domains}

    def is_open(self, domain: str) -> bool:
        if self._state.get(domain) == "OPEN":
            if time.time() > self._trip_at.get(domain, 0):
                self._state[domain] = "HALF_OPEN"
                log.info("circuit_half_open", domain=domain)
                return False
            return True
        return False

    def trip(self, domain: str):
        self._fails[domain] = self._fails.get(domain, 0) + 1
        if self._fails[domain] > self.TRIP_THRESHOLD:
            self._state[domain]   = "OPEN"
            self._trip_at[domain] = time.time() + self.RESET_AFTER
            self._fails[domain]   = 0
            log.error("circuit_tripped", domain=domain, reset_in=self.RESET_AFTER)

    def heal(self, domain: str):
        self._fails[domain] = 0
        if self._state.get(domain) in ("OPEN", "HALF_OPEN"):
            self._state[domain] = "CLOSED"
            log.info("circuit_healed", domain=domain)

DOMAINS = ["google", "bing", "ddg", "telegram"]

# ─────────────────────────────────────────────
#  REDIS WRAPPER — FIX #3 (type consistency)
# ─────────────────────────────────────────────
class RedisClient:
    """
    Thin wrapper ensuring all values are bytes consistently.
    decode_responses=False always → bytes in, bytes out.
    No implicit string/bytes confusion anywhere.
    """

    def __init__(self):
        self._r: Optional[aioredis.Redis] = None
        self.disabled = False

    async def connect(self, url: str):
        try:
            self._r = await aioredis.from_url(
                url,
                encoding="utf-8",
                decode_responses=False,   # always bytes
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            await self._r.ping()
            log.info("redis_connected")
        except Exception as e:
            log.warning("redis_unavailable", err=str(e))
            self._r = None
            self.disabled = True

    async def get(self, key: str) -> Optional[bytes]:
        if not self._r:
            return None
        try:
            val = await self._r.get(key)
            return bytes(val) if val is not None else None
        except Exception as e:
            log.warning("redis_get_fail", key=key, err=str(e))
            return None

    async def set(self, key: str, value: bytes, ttl: int = 7200):
        if not self._r:
            return
        try:
            await self._r.setex(key, ttl, value)
        except Exception as e:
            log.warning("redis_set_fail", key=key, err=str(e))

    async def exists(self, key: str) -> bool:
        if not self._r:
            return False
        try:
            return bool(await self._r.exists(key))
        except Exception:
            return False

    async def rate_limit(self, key: str, ttl: int = 5) -> bool:
        """Returns True if rate limited (key already exists)."""
        if not self._r:
            return False
        try:
            # SET NX (only set if not exists) → atomic check+set
            result = await self._r.set(key, b"1", ex=ttl, nx=True)
            return result is None  # None = key existed = rate limited
        except Exception:
            return False

    async def close(self):
        if self._r:
            await self._r.close()
            self._r = None

# ─────────────────────────────────────────────
#  DOWNLOAD QUEUE ITEM — FIX #5 (enforce structure)
# ─────────────────────────────────────────────
from dataclasses import dataclass

@dataclass
class DownloadTask:
    source:  str
    chat_id: int
    url:     str
    title:   str
    user_id: int

    def to_dict(self) -> Dict:
        return {
            "source":  self.source,
            "chat_id": self.chat_id,
            "url":     self.url,
            "title":   self.title,
            "user_id": self.user_id,
        }

    @classmethod
    def from_dict(cls, d: Dict) -> "DownloadTask":
        return cls(
            source=d.get("source", "restored"),
            chat_id=d["chat_id"],
            url=d["url"],
            title=d["title"],
            user_id=d["user_id"],
        )

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
    "DSSSB":  {"label": "🏛️ DSSSB Pharmacist",  "regions": ["DSSSB Delhi"]},
    "RUHS":   {"label": "🎓 RUHS Pharmacist",    "regions": ["RUHS Rajasthan"]},
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
    "GPAT":       {"label": "📚 GPAT / NIPER",       "regions": ["GPAT All India"]},
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

REGION_ID_MAP: OrderedDict = OrderedDict()

def get_region_hash(region_str: str) -> str:
    rh = hashlib.md5(region_str.encode()).hexdigest()[:8]
    REGION_ID_MAP[rh] = region_str
    if len(REGION_ID_MAP) > 1000:
        REGION_ID_MAP.popitem(last=False)
    return rh

def make_search_id(exam_key: str, region: str, year: int, mat: str) -> str:
    return hashlib.sha256(f"{exam_key}|{region}|{year}|{mat}".encode()).hexdigest()[:24]

def make_search_id_v2(exam_key: str, region: str, year: int, mat: str, user_id=None) -> str:
    return hashlib.sha256(f"{exam_key}|{region}|{year}|{mat}|{user_id or 0}".encode()).hexdigest()[:24]

# ─────────────────────────────────────────────
#  COMPRESSION
# ─────────────────────────────────────────────
def compress(data: Any) -> bytes:
    return zlib.compress(json.dumps(data, ensure_ascii=False).encode("utf-8"), level=6)

def decompress(b: bytes) -> Any:
    return json.loads(zlib.decompress(b).decode("utf-8"))

# ─────────────────────────────────────────────
#  SQLITE SUBSYSTEM
# ─────────────────────────────────────────────
SQLITE_DB_PATH = "sessions_spillover.db"

class SQLiteStore:
    """Async SQLite with WAL mode and a single shared lock."""

    def __init__(self, path: str):
        self.path = path
        self._lock: Optional[asyncio.Lock] = None

    async def async_init(self):
        self._lock = asyncio.Lock()
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute("PRAGMA journal_mode=WAL")
                await db.execute(
                    "CREATE TABLE IF NOT EXISTS spillover "
                    "(sid TEXT PRIMARY KEY, data BLOB, ts REAL)"
                )
                await db.execute(
                    "CREATE TABLE IF NOT EXISTS persistent_metrics "
                    "(id TEXT PRIMARY KEY, data TEXT)"
                )
                await db.commit()
        log.info("sqlite_initialized", path=self.path)

    async def save_session(self, sid: str, data: bytes):
        async with self._lock:
            try:
                async with aiosqlite.connect(self.path) as db:
                    await db.execute(
                        "REPLACE INTO spillover (sid, data, ts) VALUES (?, ?, ?)",
                        (sid, data, time.time()),
                    )
                    await db.commit()
            except Exception as e:
                log.error("sqlite_save_fail", err=str(e))

    async def load_session(self, sid: str) -> Optional[bytes]:
        async with self._lock:
            try:
                async with aiosqlite.connect(self.path) as db:
                    async with db.execute(
                        "SELECT data FROM spillover WHERE sid=?", (sid,)
                    ) as cur:
                        row = await cur.fetchone()
                        # FIX #5 (BLOB): aiosqlite returns memoryview for BLOB
                        return bytes(row[0]) if row else None
            except Exception as e:
                log.error("sqlite_load_fail", err=str(e))
                return None

    async def save_metrics(self, payload: Dict):
        async with self._lock:
            try:
                async with aiosqlite.connect(self.path) as db:
                    await db.execute(
                        "REPLACE INTO persistent_metrics (id, data) VALUES (?, ?)",
                        ("bot_metrics_v3", json.dumps(payload)),
                    )
                    await db.commit()
            except Exception as e:
                log.error("sqlite_metrics_save_fail", err=str(e))

    async def load_metrics(self) -> Optional[Dict]:
        async with self._lock:
            try:
                async with aiosqlite.connect(self.path) as db:
                    async with db.execute(
                        "SELECT data FROM persistent_metrics WHERE id=?",
                        ("bot_metrics_v3",),
                    ) as cur:
                        row = await cur.fetchone()
                        return json.loads(row[0]) if row else None
            except Exception as e:
                log.error("sqlite_metrics_load_fail", err=str(e))
                return None

# ─────────────────────────────────────────────
#  BOT STATE — single container, no module-level
#  asyncio primitives.  FIX #8
# ─────────────────────────────────────────────
class BotState:
    """
    All mutable bot state lives here.
    Instantiated inside main() after event loop starts.
    No asyncio primitive is created at import time.
    """

    def __init__(self):
        # Async primitives — set in async_init()
        self.state_lock:     asyncio.Lock
        self.cache_lock:     asyncio.Lock
        self.active_dl_lock: asyncio.Lock
        self.download_queue: asyncio.Queue
        self.dl_semaphore:   asyncio.Semaphore
        self.scrape_semaphore: asyncio.Semaphore
        self.domain_limits:  Dict[str, asyncio.Semaphore]

        # Sync state
        self.global_bans:    set = set()
        self.user_violations: Dict[int, int] = {}
        self.user_dl_counts:  Dict[int, int] = {}
        self.user_daily_quota: Dict[int, int] = {}
        self.user_dl_ts:      Dict[int, float] = {}

        self.local_mem_cache: Dict[str, Any] = {}

        self.session_map:     OrderedDict = OrderedDict()
        self.session_expiry:  Dict[str, float] = {}
        self.session_last_accessed: Dict[str, float] = {}
        self.session_ttl = 3600

        self.active_dl    = 0
        self.burst_workers = 0
        self.burst_tasks:  set = set()
        self.last_burst_time = 0.0
        self.burst_cooldown  = 30.0

        self.queue_paused   = False
        self.start_time     = time.time()
        self.last_ban_clear = time.time()

        self.metrics: Dict = {
            "total_searches":  0,
            "pdfs_downloaded": 0,
            "bytes_downloaded": 0,
            "api_fallback_hits": 0,
            "engine_errors": {d: 0 for d in DOMAINS},
        }
        self.mongo_synced_sids: set = set()
        self.dl_latency_ema   = 0.0
        self.event_loop_lag   = 0.0

        # Task registry
        self.task_registry: Dict[str, set] = {
            "static_workers":   set(),
            "burst_workers":    set(),
            "background_loops": set(),
        }

    async def async_init(self):
        """Called inside running event loop."""
        self.state_lock      = asyncio.Lock()
        self.cache_lock      = asyncio.Lock()
        self.active_dl_lock  = asyncio.Lock()
        self.download_queue  = asyncio.Queue(maxsize=1000)
        self.dl_semaphore    = asyncio.Semaphore(5)
        self.scrape_semaphore = asyncio.Semaphore(8)
        self.domain_limits   = {
            "google":   asyncio.Semaphore(2),
            "bing":     asyncio.Semaphore(3),
            "ddg":      asyncio.Semaphore(4),
            "telegram": asyncio.Semaphore(3),
        }
        log.info("bot_state_async_init_done")

    # ── ban helpers ──────────────────────────
    async def is_banned(self, user_id: int) -> bool:
        async with self.state_lock:
            return user_id in self.global_bans

    async def record_violation(self, user_id: int):
        async with self.state_lock:
            self.user_violations[user_id] = self.user_violations.get(user_id, 0) + 1
            if self.user_violations[user_id] > 15:
                self.global_bans.add(user_id)
                log.warning("global_ban_applied", user_id=user_id)

    # ── queue helpers ────────────────────────
    def dump_queue(self) -> List[Dict]:
        items = []
        while True:
            try:
                task = self.download_queue.get_nowait()
                if isinstance(task, DownloadTask):
                    items.append(task.to_dict())
                # task_done only for items we actually consumed
                self.download_queue.task_done()
            except asyncio.QueueEmpty:
                break
            except Exception:
                break
        return items

    async def restore_queue(self, items: List[Dict]):
        loaded = 0
        for d in items:
            try:
                task = DownloadTask.from_dict(d)
                self.download_queue.put_nowait(task)
                loaded += 1
            except asyncio.QueueFull:
                log.warning("queue_full_on_restore", loaded=loaded, dropped=len(items) - loaded)
                break
            except Exception as e:
                log.warning("queue_restore_item_fail", err=str(e))
        log.info("queue_restored", count=loaded)

# ─────────────────────────────────────────────
#  GLOBAL SINGLETONS — created in main()
# ─────────────────────────────────────────────
state:    BotState
proxy_pool: ProxyPool
circuit:    CircuitBreaker
redis_client: RedisClient
sqlite_store: SQLiteStore
SESSION:  Optional[aiohttp.ClientSession] = None
MONGO_DB: Any = None

# ─────────────────────────────────────────────
#  PYROGRAM CLIENT
# ─────────────────────────────────────────────
app = Client(
    "pharma_ultimate_v3",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

# ─────────────────────────────────────────────
#  SAFE HANDLER DECORATOR — FIX #9
#
#  FIX: channel posts have from_user=None → guard
#  FIX: None user_id with empty ALLOWED_USERS list
#       must NOT silently pass (could allow anon)
# ─────────────────────────────────────────────
def safe(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        user_id: Optional[int] = None
        for a in args:
            if isinstance(a, Message) and a.from_user:
                user_id = a.from_user.id
                break
            elif isinstance(a, CallbackQuery) and a.from_user:
                user_id = a.from_user.id
                break

        # FIX #9: if user_id is None (channel/anon) AND we have an allowlist,
        # block it — anonymous posts should not bypass access control.
        if ALLOWED_USERS:
            if user_id is None or (user_id not in ALLOWED_USERS and user_id not in ADMIN_IDS):
                log.warning("access_denied", user_id=user_id)
                return

        # Rate-limit / ban check (no lock held across await)
        if user_id and await state.is_banned(user_id):
            return

        tid = uuid.uuid4().hex[:8]
        try:
            return await func(*args, **kwargs)
        except FloodWait as fw:
            log.warning("flood_wait", fn=func.__name__, secs=fw.value, tid=tid)
            await asyncio.sleep(fw.value + 1)
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                log.error("retry_after_flood_fail", fn=func.__name__, err=str(e), tid=tid)
        except MessageNotModified:
            pass
        except Exception as e:
            log.error("handler_crash", fn=func.__name__, err=str(e), tid=tid)
            for a in args:
                if isinstance(a, Message):
                    try:
                        await a.reply_text(f"❌ Error — Trace: `{tid}`")
                    except Exception:
                        pass
                    break
                elif isinstance(a, CallbackQuery):
                    try:
                        await a.answer(f"⚠️ Error — Trace: {tid}", show_alert=True)
                    except Exception:
                        pass
                    break
    return wrapper

# ─────────────────────────────────────────────
#  RATE LIMITER — FIX #10 (no lock across await)
# ─────────────────────────────────────────────
async def is_rate_limited(user_id: int, prefix: str, ttl: int = 5) -> bool:
    if await state.is_banned(user_id):
        return True
    key = f"rl:{prefix}:{user_id}"
    limited = await redis_client.rate_limit(key, ttl)
    if limited:
        await state.record_violation(user_id)
    return limited

# ─────────────────────────────────────────────
#  HTTP FETCH with per-request UA — FIX #4
# ─────────────────────────────────────────────
async def fetch(
    url: str,
    *,
    proxy: Optional[str] = None,
    timeout: float = 20.0,
    domain: Optional[str] = None,
) -> Optional[str]:
    """
    Single fetch with:
    - Per-request random User-Agent (FIX #4)
    - Proxy success/fail tracking
    - Circuit breaker integration
    - SSRF-safe session
    """
    if domain and circuit.is_open(domain):
        log.warning("circuit_open_skip", domain=domain, url=url)
        return None

    if not valid_url(url):
        log.warning("invalid_url_blocked", url=url)
        return None

    t_start = time.monotonic()
    try:
        async with SESSION.get(
            url,
            proxy=proxy,
            timeout=aiohttp.ClientTimeout(total=timeout),
            headers={"User-Agent": random_ua()},   # FIX #4: per-request UA
            allow_redirects=True,
            max_redirects=5,
        ) as resp:
            resp.raise_for_status()
            text = await resp.text(errors="replace")

        latency = time.monotonic() - t_start
        if proxy:
            await proxy_pool.success(proxy, latency)
        if domain:
            circuit.heal(domain)
        return text

    except aiohttp.ClientResponseError as e:
        log.warning("fetch_http_error", url=url, status=e.status)
        if proxy:
            await proxy_pool.fail(proxy)
        if domain:
            circuit.trip(domain)
        return None
    except Exception as e:
        log.warning("fetch_error", url=url, err=str(e)[:80])
        if proxy:
            await proxy_pool.fail(proxy)
        if domain:
            circuit.trip(domain)
        return None

# ─────────────────────────────────────────────
#  SESSION INIT — FIX #6 (resolver failure safe)
# ─────────────────────────────────────────────
async def init_session():
    global SESSION
    try:
        connector = aiohttp.TCPConnector(
            resolver=SSRFSafeResolver(),   # FIX #1 & #6
            ssl=True,
            limit=100,
            limit_per_host=10,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        SESSION = aiohttp.ClientSession(
            connector=connector,
            # FIX #4: no static UA header here — set per-request in fetch()
        )
        log.info("aiohttp_session_created")
    except Exception as e:
        log.error("aiohttp_session_init_fail", err=str(e))
        # Fallback: session without custom resolver (still functional)
        SESSION = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
        )
        log.warning("aiohttp_session_fallback_no_ssrf_resolver")

async def close_session():
    global SESSION
    if SESSION and not SESSION.closed:
        await SESSION.close()
    SESSION = None

# ─────────────────────────────────────────────
#  MONGO INIT — FIX #10 (timeout + ping safe)
# ─────────────────────────────────────────────
async def init_mongo():
    global MONGO_DB
    try:
        client = AsyncIOMotorClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=10000,
        )
        # Motor's admin.command is truly async
        await asyncio.wait_for(
            client.admin.command("ping"),
            timeout=6.0,   # FIX #10: explicit outer timeout
        )
        db_name = urlparse(MONGO_URI).path.lstrip("/") or "pharma_bot"
        MONGO_DB = client[db_name]
        log.info("mongo_connected", db=db_name)
    except asyncio.TimeoutError:
        log.warning("mongo_ping_timeout")
        MONGO_DB = None
    except Exception as e:
        log.warning("mongo_unavailable", err=str(e))
        MONGO_DB = None

# ─────────────────────────────────────────────
#  QUEUE PERSISTENCE
# ─────────────────────────────────────────────
QUEUE_BACKUP_PATH = "local_queue_backup.json"

def dump_queue_sync():
    """Called at shutdown (sync context)."""
    items = state.dump_queue()
    if items:
        try:
            with open(QUEUE_BACKUP_PATH, "w") as f:
                json.dump(items, f)
            log.info("queue_saved", count=len(items))
        except Exception as e:
            log.error("queue_save_fail", err=str(e))

async def restore_queue_async():
    if not os.path.exists(QUEUE_BACKUP_PATH):
        return
    try:
        with open(QUEUE_BACKUP_PATH, "r") as f:
            items = json.load(f)
        await state.restore_queue(items)
        os.remove(QUEUE_BACKUP_PATH)
    except Exception as e:
        log.error("queue_restore_fail", err=str(e))

# ─────────────────────────────────────────────
#  TASK HELPERS
# ─────────────────────────────────────────────
def _log_task_exc(task: asyncio.Task):
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error("task_crashed", name=task.get_name(), err=str(e))

def create_safe_task(coro, name: str) -> asyncio.Task:
    task = asyncio.create_task(coro, name=name)
    task.add_done_callback(_log_task_exc)
    return task

# ─────────────────────────────────────────────
#  STARTUP / SHUTDOWN
# ─────────────────────────────────────────────
async def on_startup():
    global state, proxy_pool, circuit, redis_client, sqlite_store

    # Instantiate singletons
    state        = BotState()
    proxy_pool   = ProxyPool(PROXIES)
    circuit      = CircuitBreaker(DOMAINS)
    redis_client = RedisClient()
    sqlite_store = SQLiteStore(SQLITE_DB_PATH)

    # Init async primitives — all in running loop
    await state.async_init()
    await proxy_pool.async_init()
    await sqlite_store.async_init()

    # Load persisted data
    metrics = await sqlite_store.load_metrics()
    if metrics:
        state.metrics.update(metrics.get("metrics", {}))
        state.user_daily_quota.update(metrics.get("user_daily_quota", {}))
        state.user_dl_counts.update(metrics.get("user_dl_counts", {}))

    # Connect external services
    await init_session()
    await redis_client.connect(REDIS_URL)
    await init_mongo()
    await restore_queue_async()

    log.info("startup_complete", node=NODE_ID, proxies=proxy_pool.stats())

async def on_shutdown():
    dump_queue_sync()
    await sqlite_store.save_metrics({
        "metrics":          state.metrics,
        "user_daily_quota": state.user_daily_quota,
        "user_dl_counts":   state.user_dl_counts,
    })
    await close_session()
    await redis_client.close()
    log.info("shutdown_complete", node=NODE_ID)

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
async def main():
    await on_startup()
    try:
        await app.start()
        log.info("bot_running")
        await asyncio.Event().wait()
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