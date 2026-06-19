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

PROXIES = [p for p in os.getenv("PROXIES", "").split(",") if p.strip()]

GOOGLE_SEARCH_API_KEY = os.getenv("GOOGLE_SEARCH_API_KEY", "")
GOOGLE_SEARCH_CX = os.getenv("GOOGLE_SEARCH_CX", "")

# ─────────────────────────────────────────────
#  SMART PROXY TRACKING SYSTEM & STATE LOCKS
# ─────────────────────────────────────────────
STATE_LOCK = asyncio.Lock()
SQLITE_LOCK = asyncio.Lock()

DNS_CACHE = {}
DNS_CACHE_TTL = 3600
DNS_CACHE_LOCK = asyncio.Lock()

USER_DL_TIMESTAMPS = {}

PROXY_FAILS = {p: 0 for p in PROXIES}
PROXY_SUCCESS = {p: 0 for p in PROXIES}
PROXY_LATENCY = {p: 5.0 for p in PROXIES} 

DEAD_PROXIES = set()
PROXY_REVIVE_COUNTS = {p: 0 for p in PROXIES}
ABSOLUTE_DEAD_PROXIES = set()
PROXY_BLACKLIST_THRESHOLD = 10

DOMAIN_PROXY_STATS = {
    "google": {p: {"fails": 0, "success": 0, "latency": 5.0} for p in PROXIES},
    "bing":   {p: {"fails": 0, "success": 0, "latency": 5.0} for p in PROXIES},
    "ddg":    {p: {"fails": 0, "success": 0, "latency": 5.0} for p in PROXIES},
    "telegram":{p: {"fails": 0, "success": 0, "latency": 5.0} for p in PROXIES}
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

REGION_ID_MAP = {}
def get_region_hash(region_str: str) -> str:
    rh = hashlib.md5(region_str.encode()).hexdigest()[:8]
    REGION_ID_MAP[rh] = region_str
    if len(REGION_ID_MAP) > 1000:
        REGION_ID_MAP.pop(next(iter(REGION_ID_MAP)))
    return rh

# ─────────────────────────────────────────────
#  GLOBAL STATE
# ─────────────────────────────────────────────
SESSION:    Optional[aiohttp.ClientSession] = None
REDIS:      Optional[aioredis.Redis]        = None
MONGO_DB                                    = None

DOWNLOAD_QUEUE     = asyncio.Queue(maxsize=1000)
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(5)
SCRAPE_SEMAPHORE   = asyncio.Semaphore(8)   

DOMAIN_LIMITS = {
    "google": asyncio.Semaphore(2),
    "bing": asyncio.Semaphore(3),
    "ddg": asyncio.Semaphore(4),
    "telegram": asyncio.Semaphore(3)
}

DOMAIN_PENALTY = {
    "google": 0.0,
    "bing": 0.0,
    "ddg": 0.0,
    "telegram": 0.0
}

DOMAIN_CIRCUIT_BREAKER = {"google": 0, "bing": 0, "ddg": 0, "telegram": 0}
CIRCUIT_TRIP_TIME = {"google": 0.0, "bing": 0.0, "ddg": 0.0, "telegram": 0.0}
CIRCUIT_STATE = {"google": "CLOSED", "bing": "CLOSED", "ddg": "CLOSED", "telegram": "CLOSED"}

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

ACTIVE_DL      = 0
BURST_WORKERS  = 0
MAX_BURST_WORKERS = 10 
MAX_TOTAL_WORKERS = 15
BURST_TASKS    = set()

LAST_BURST_TIME = 0.0
BURST_COOLDOWN = 30.0 

LOCAL_MEM_CACHE: Dict[str, Dict] = {}
CACHE_LOCK = asyncio.Lock()

TASK_REGISTRY = {
    "static_workers": set(),
    "burst_workers": set(),
    "background_loops": set()
}

BOT_METRICS = {
    "total_searches": 0,
    "pdfs_downloaded": 0,
    "bytes_downloaded": 0,
    "api_fallback_hits": 0,
    "engine_errors": {
        "google": 0,
        "bing": 0,
        "ddg": 0,
        "telegram": 0
    }
}
MONGO_SYNCED_SIDS = set() 

DOWNLOAD_LATENCY_EMA = 0.0
EVENT_LOOP_LAG = 0.0

ACTIVE_DL_LOCK = asyncio.Lock()

QUEUE_PAUSED   = False
BOT_START_TIME = time.time()
LAST_BAN_CLEAR_TIME = time.time()

SESSION_MAP: Dict[str, bytes] = OrderedDict() 
SESSION_EXPIRY = {}
SESSION_LAST_ACCESSED: Dict[str, float] = {} 
SESSION_TTL = 3600
REDIS_DISABLED_WARNING = False

GLOBAL_BANS = set()
USER_VIOLATIONS = {}
USER_DL_COUNTS = {}
USER_DAILY_QUOTA = {}

# ─────────────────────────────────────────────
#  100% ASYNC AIOSQLITE SUBSYSTEM
# ─────────────────────────────────────────────
SQLITE_DB_PATH = "sessions_spillover.db"

async def init_sqlite_spillover():
    async with SQLITE_LOCK:
        try:
            async with aiosqlite.connect(SQLITE_DB_PATH) as db:
                await db.execute('''CREATE TABLE IF NOT EXISTS spillover
                             (sid TEXT PRIMARY KEY, data BLOB, ts REAL)''')
                await db.execute('''CREATE TABLE IF NOT EXISTS persistent_metrics
                             (id TEXT PRIMARY KEY, data TEXT)''')
                await db.commit()
        except Exception as e:
            log.error("sqlite_init_fail", err=str(e))

async def sqlite_save(sid: str, data: bytes):
    async with SQLITE_LOCK:
        try:
            async with aiosqlite.connect(SQLITE_DB_PATH) as db:
                await db.execute("REPLACE INTO spillover (sid, data, ts) VALUES (?, ?, ?)", (sid, data, time.time()))
                await db.commit()
        except Exception as e:
            log.error("sqlite_spillover_save_fail", err=str(e)[:60])

async def sqlite_load(sid: str) -> Optional[bytes]:
    async with SQLITE_LOCK:
        try:
            async with aiosqlite.connect(SQLITE_DB_PATH) as db:
                async with db.execute("SELECT data FROM spillover WHERE sid=?", (sid,)) as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else None
        except Exception:
            return None

async def save_persistent_metrics():
    payload = {
        "BOT_METRICS": BOT_METRICS,
        "USER_DAILY_QUOTA": USER_DAILY_QUOTA,
        "USER_DL_COUNTS": USER_DL_COUNTS
    }
    async with SQLITE_LOCK:
        try:
            async with aiosqlite.connect(SQLITE_DB_PATH) as db:
                await db.execute("REPLACE INTO persistent_metrics (id, data) VALUES (?, ?)", ("bot_metrics_v2", json.dumps(payload)))
                await db.commit()
        except Exception:
            pass

async def load_persistent_metrics():
    async with SQLITE_LOCK:
        try:
            async with aiosqlite.connect(SQLITE_DB_PATH) as db:
                async with db.execute("SELECT data FROM persistent_metrics WHERE id=?", ("bot_metrics_v2",)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        loaded = json.loads(row[0])
                        BOT_METRICS.update(loaded.get("BOT_METRICS", {}))
                        USER_DAILY_QUOTA.update(loaded.get("USER_DAILY_QUOTA", {}))
                        USER_DL_COUNTS.update(loaded.get("USER_DL_COUNTS", {}))
        except Exception:
            pass

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
            if isinstance(item, tuple) and len(item) == 5:
                items.append({"chat_id": item[1], "url": item[2], "title": item[3], "user_id": item[4]})
            DOWNLOAD_QUEUE.task_done()
        except Exception:
            pass
    if items:
        try:
            with open("local_queue_backup.json", "w") as f:
                json.dump(items, f)
            log.info("local_queue_saved_to_disk", count=len(items))
        except Exception:
            pass

def load_local_queue():
    if os.path.exists("local_queue_backup.json"):
        try:
            with open("local_queue_backup.json", "r") as f:
                items = json.load(f)
            for item in items:
                try:
                    DOWNLOAD_QUEUE.put_nowait(("restored_app", item["chat_id"], item["url"], item["title"], item["user_id"]))
                except asyncio.QueueFull:
                    log.warning("local_queue_full_during_restore", dropped=len(items) - items.index(item))
                    break
            os.remove("local_queue_backup.json")
            log.info("local_queue_restored_from_disk", count=len(items))
        except Exception:
            pass

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
#  SAFE HANDLER DECORATOR & FRIENDS ONLY AUTH 
# ─────────────────────────────────────────────
def safe(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        user_id = None
        for a in args:
            if isinstance(a, Message):
                user_id = a.from_user.id
                break
            elif isinstance(a, CallbackQuery):
                user_id = a.from_user.id
                break
                
        if ALLOWED_USERS and user_id and user_id not in ALLOWED_USERS and user_id not in ADMIN_IDS:
            log.warning("unauthorized_access_attempt", user_id=user_id)
            return

        tid = uuid.uuid4().hex[:8]
        try:
            return await func(*args, **kwargs)
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
            try:
                return await func(*args, **kwargs)
            except Exception:
                pass
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
# ─────────────────────────────────────────────
async def is_rate_limited(user_id: int, prefix: str, ttl: int = 5) -> bool:
    async with STATE_LOCK:
        if user_id in GLOBAL_BANS:
            return True
            
    if not REDIS:
        return False
    key = f"rl:{prefix}:{user_id}"
    if await REDIS.get(key):
        async with STATE_LOCK:
            USER_VIOLATIONS[user_id] = USER_VIOLATIONS.get(user_id, 0) + 1
            if USER_VIOLATIONS[user_id] > 15:
                GLOBAL_BANS.add(user_id)
                log.warning("global_ban_applied_to_spammer", user_id=user_id)
        return True
    await REDIS.setex(key, ttl, b"1")
    return False

async def redis_get(key: str) -> Optional[bytes]:
    if not REDIS:
        return None
    try:
        val = await REDIS.get(key)
        return val  
    except Exception:
        return None

async def redis_set(key: str, value: bytes, ttl: int = 7200):
    if not REDIS:
        return
    try:
        await REDIS.setex(key, ttl, value)
    except Exception:
        pass

# ─────────────────────────────────────────────
#  COMPRESSION HELPERS
# ─────────────────────────────────────────────
def compress(data: list) -> bytes:
    return zlib.compress(json.dumps(data, ensure_ascii=False).encode("utf-8"), level=6)

def decompress(b: bytes) -> list:
    return json.loads(zlib.decompress(b).decode("utf-8"))

def normalize_bytes(x):
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
#  SSRF-SAFE & SMART PROXY FETCH 
# ─────────────────────────────────────────────
_PRIVATE_PREFIXES = (
    "10.", "172.16.", "172.17.", "172.18.", "172.19.", "172.20.",
    "172.21.", "172.22.", "172.23.", "172.24.", "172.25.", "172.26.",
    "172.27.", "172.28.", "172.29.", "172.30.", "172.31.", "192.168.",
)
_PRIVATE_EXACT = {"127.0.0.1", "0.0.0.0", "::1", "localhost"}

def _host_is_safe(host: str) -> bool:
    h = host.lower().split(":")[0]
    h = h.strip("[]")
    if h in _PRIVATE_EXACT:
        return False
    if any(h.startswith(p) for p in _PRIVATE_PREFIXES):
        return False
    return True

async def async_valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https"): return False
        
        h = safe_host(p.hostname or "")
        if not h: return False
        
        clean_host = (p.hostname or "").lower().strip("[]")
        
        async with DNS_CACHE_LOCK:
            if clean_host in DNS_CACHE and time.time() - DNS_CACHE[clean_host]['ts'] < DNS_CACHE_TTL:
                return DNS_CACHE[clean_host]['safe']
                
        addr_info = await asyncio.to_thread(socket.getaddrinfo, clean_host, None)
        is_safe = True
        for res in addr_info:
            ip = res[4][0]
            if ipaddress.ip_address(ip).is_private or ipaddress.ip_address(ip).is_loopback:
                is_safe = False
                break
                
        async with DNS_CACHE_LOCK:
            DNS_CACHE[clean_host] = {'safe': is_safe, 'ts': time.time()}
            
        return is_safe
    except Exception:
        return False

def safe_host(host: str) -> bool:
    clean_host = host.split(":")[0].strip("[]")
    if not _host_is_safe(clean_host):
        return False
    return True

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
            ip_obj = ipaddress.ip_address(ip['host'])
            if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local:
                raise ValueError(f"SSRF Blocked at DNS level: {host} -> {ip['host']}")
        return ips

async def get_random_proxy() -> Optional[str]:
    async with STATE_LOCK:
        if not PROXIES:
            return None
        valid_proxies = [p for p in PROXIES if p not in DEAD_PROXIES and PROXY_FAILS.get(p, 0) < 3]
        if not valid_proxies:
            log.warning("proxy_pool_exhausted_resetting")
            for p in PROXIES:
                if p not in DEAD_PROXIES:
                    PROXY_FAILS[p] = 0
                    PROXY_LATENCY[p] = 5.0
            valid_proxies = [p for p in PROXIES if p not in DEAD_PROXIES]
            if not valid_proxies:
                return None 
        
        valid_proxies.sort(key=lambda p: (PROXY_LATENCY.get(p, 5.0), -PROXY_SUCCESS.get(p, 0)))
        return random.choice(valid_proxies[:3])

async def get_domain_aware_proxy(domain: str) -> Optional[str]:
    async with STATE_LOCK:
        if not PROXIES or domain not in DOMAIN_PROXY_STATS:
            pass 
    
    if not PROXIES or domain not in DOMAIN_PROXY_STATS:
        return await get_random_proxy()

    async with STATE_LOCK:
        valid_proxies = [p for p in PROXIES if p not in DEAD_PROXIES and DOMAIN_PROXY_STATS[domain][p]["fails"] < 3]
        
    if not valid_proxies:
        return await get_random_proxy()
        
    async with STATE_LOCK:
        valid_proxies.sort(key=lambda p: (DOMAIN_PROXY_STATS[domain][p]["latency"], -DOMAIN_PROXY_STATS[domain][p]["success"]))
        return random.choice(valid_proxies[:3])

async def mark_domain_proxy_failed(domain: str, proxy: str):
    async with STATE_LOCK:
        if proxy and domain in DOMAIN_PROXY_STATS and proxy in DOMAIN_PROXY_STATS[domain]:
            DOMAIN_PROXY_STATS[domain][proxy]["fails"] += 1
            DOMAIN_PROXY_STATS[domain][proxy]["latency"] += 2.0

async def mark_domain_proxy_success(domain: str, proxy: str, latency: float):
    async with STATE_LOCK:
        if proxy and domain in DOMAIN_PROXY_STATS and proxy in DOMAIN_PROXY_STATS[domain]:
            DOMAIN_PROXY_STATS[domain][proxy]["fails"] = 0
            DOMAIN_PROXY_STATS[domain][proxy]["success"] += 1
            curr_lat = DOMAIN_PROXY_STATS[domain][proxy]["latency"]
            DOMAIN_PROXY_STATS[domain][proxy]["latency"] = (curr_lat * 0.7) + (latency * 0.3)

async def mark_proxy_failed(proxy: str):
    async with STATE_LOCK:
        if proxy and proxy in PROXY_FAILS:
            PROXY_FAILS[proxy] += 1
            PROXY_LATENCY[proxy] += 2.0 
            log.warning("proxy_failed", proxy=proxy, total_fails=PROXY_FAILS[proxy])
            if PROXY_FAILS[proxy] > PROXY_BLACKLIST_THRESHOLD:
                DEAD_PROXIES.add(proxy)
                log.error("proxy_blacklisted_permanently", proxy=proxy)

async def mark_proxy_success(proxy: str, latency: float):
    async with STATE_LOCK:
        if proxy and proxy in PROXY_FAILS:
            PROXY_FAILS[proxy] = 0
            PROXY_SUCCESS[proxy] += 1
            PROXY_LATENCY[proxy] = (PROXY_LATENCY[proxy] * 0.7) + (latency * 0.3)

async def safe_get(url: str, headers: Optional[dict] = None,
                   timeout: Optional[aiohttp.ClientTimeout] = None, explicit_proxy: str = None, domain_marker: str = None, retries: int = 2) -> Optional[aiohttp.ClientResponse]:
    if not await async_valid_url(url):
        return None
    h = headers or {"User-Agent": random.choice(USER_AGENTS)}
    t = timeout or aiohttp.ClientTimeout(total=25)
    
    for attempt in range(retries + 1):
        req_proxy = explicit_proxy or await get_random_proxy()
        start_time = time.time()
        resp = None
        
        try:
            resp = await SESSION.get(url, headers=h, timeout=t, allow_redirects=True, proxy=req_proxy)
            latency = time.time() - start_time
            
            if req_proxy and not str(req_proxy).startswith("https"):
                log.info("insecure_proxy_used_beware_mitm", proxy=req_proxy)

            if resp.status in (429, 403, 500, 502, 503, 504) or "captcha" in str(resp.url).lower():
                await mark_proxy_failed(req_proxy)
                if domain_marker: await mark_domain_proxy_failed(domain_marker, req_proxy)
                
                if attempt < retries:
                    await asyncio.sleep(2 ** attempt)
                    if resp and not resp.closed: resp.close()
                    continue
            else:
                await mark_proxy_success(req_proxy, latency)
                if domain_marker: await mark_domain_proxy_success(domain_marker, req_proxy, latency)

            if resp.history:
                for r in resp.history:
                    if not await async_valid_url(str(r.url)):
                        if resp and not resp.closed: resp.close()
                        log.warning("ssrf_redirect_history_blocked_async", url=str(r.url))
                        return None
                        
            final_url = str(resp.url)
            if not await async_valid_url(final_url):
                if resp and not resp.closed: resp.close()
                log.warning("ssrf_redirect_blocked_async", final=final_url)
                return None
            return resp
        except Exception as e:
            await mark_proxy_failed(req_proxy)
            if domain_marker: await mark_domain_proxy_failed(domain_marker, req_proxy)
            log.warning("safe_get_fail_attempt", url=url[:80], err=str(e)[:60], attempt=attempt)
            if attempt < retries:
                if resp and not resp.closed: resp.close() 
                await asyncio.sleep(2 ** attempt)
            else:
                if resp and not resp.closed: resp.close()
                return None
    return None

# ─────────────────────────────────────────────
#  QUERY BUILDER
# ─────────────────────────────────────────────
def build_queries(exam_label: str, region: str, year: Optional[int], mat: str) -> List[str]:
    clean = re.sub(r"[^\w\s]", "", exam_label).strip()
    yr = str(year) if year else ""

    base = {
        "pyq": [
            f"{clean} {region} pharmacist previous year question paper {yr} pdf",
            f"{clean} pharmacist PYQ {yr} pdf download",
            f"{clean} {region} pharmacist solved paper {yr}",
            f"{clean} pharmacist question paper {yr} memory based pdf",
        ],
        "syllabus": [
            f"{clean} pharmacist syllabus {yr} pdf",
            f"{clean} {region} pharmacist exam pattern syllabus",
            f"{clean} pharmacist detailed syllabus pdf download",
        ],
        "notes": [
            f"{clean} pharmacist study material {yr} pdf",
            f"{clean} pharmacist notes pdf download",
            f"pharmacist competitive exam handwritten notes pdf {yr}",
            f"pharmacy important notes {yr} pdf free download",
        ],
        "anskey": [
            f"{clean} {region} pharmacist answer key {yr} pdf",
            f"{clean} pharmacist official answer key {yr}",
        ],
        "mock": [
            f"{clean} pharmacist mock test {yr} pdf",
            f"pharmacist model question paper {yr} pdf",
            f"pharmacy competitive exam practice set pdf",
        ],
        "books": [
            f"pharmacist competitive exam book pdf free download",
            f"RPS Malik pharmacist book pdf",
            f"pharmacy D Pharma B Pharma notes pdf",
            f"pharmacist exam reference book pdf",
        ],
    }
    return base.get(mat, base["pyq"])

# ─────────────────────────────────────────────
#  ADAPTIVE DOMAIN DELAY HELPER
# ─────────────────────────────────────────────
async def adaptive_domain_sleep(domain: str):
    penalty = DOMAIN_PENALTY.get(domain, 0.0)
    if penalty > 0:
        await asyncio.sleep(penalty)

def apply_domain_penalty(domain: str):
    DOMAIN_PENALTY[domain] = min(DOMAIN_PENALTY.get(domain, 0.0) + 0.5, 5.0)

def relieve_domain_penalty(domain: str):
    DOMAIN_PENALTY[domain] = max(DOMAIN_PENALTY.get(domain, 0.0) - 0.1, 0.0)

# ─────────────────────────────────────────────
#  SCRAPERS 
# ─────────────────────────────────────────────
def match_keywords(text: str, kw_words: list) -> bool:
    return any(w in text.lower() for w in kw_words)

async def scrape_ddg(query: str) -> List[Dict]:
    if is_circuit_open("ddg"): return [] 
    await adaptive_domain_sleep("ddg")
    async with DOMAIN_LIMITS["ddg"]:
        async with SCRAPE_SEMAPHORE:
            results = []
            resp = None
            try:
                url = "https://html.duckduckgo.com/html/?q=" + quote_plus(query + " filetype:pdf")
                resp = await safe_get(url, headers={
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://duckduckgo.com/",
                }, explicit_proxy=await get_domain_aware_proxy("ddg"), domain_marker="ddg")
                if not resp:
                    apply_domain_penalty("ddg")
                    trip_circuit("ddg")
                    return []
                relieve_domain_penalty("ddg")
                heal_circuit("ddg")
                try:
                    async with resp:
                        html = await resp.text()
                except Exception:
                    if resp and not resp.closed: resp.close()
                    raise
                soup = BeautifulSoup(html, "html.parser")
                for i, a in enumerate(soup.find_all("a", class_="result__a")):
                    if i % 10 == 0: await asyncio.sleep(0)  
                    href = a.get("href", "")
                    if "uddg=" in href:
                        href = unquote(href.split("uddg=")[1].split("&")[0])
                    if href.startswith("http") and valid_url(href):
                        results.append({"title": a.get_text(strip=True)[:140], "url": href, "source": "DDG"})
            except Exception as e:
                trip_circuit("ddg")
                BOT_METRICS["engine_errors"]["ddg"] += 1
                apply_domain_penalty("ddg")
                log.warning("ddg_fail", err=str(e)[:80])
            finally:
                if resp and not resp.closed: resp.close()
            return results

async def scrape_bing(query: str) -> List[Dict]:
    if is_circuit_open("bing"): return []
    await adaptive_domain_sleep("bing")
    async with DOMAIN_LIMITS["bing"]:
        async with SCRAPE_SEMAPHORE:
            results = []
            resp = None
            try:
                url = "https://www.bing.com/search?q=" + quote_plus(query + " filetype:pdf") + "&count=30"
                resp = await safe_get(url, explicit_proxy=await get_domain_aware_proxy("bing"), domain_marker="bing")
                if not resp:
                    apply_domain_penalty("bing")
                    trip_circuit("bing")
                    return []
                relieve_domain_penalty("bing")
                heal_circuit("bing")
                try:
                    async with resp:
                        html = await resp.text()
                except Exception:
                    if resp and not resp.closed: resp.close()
                    raise
                soup = BeautifulSoup(html, "html.parser")
                for i, li in enumerate(soup.find_all("li", class_="b_algo")):
                    if i % 10 == 0: await asyncio.sleep(0)  
                    a = li.find("a")
                    if a:
                        href = a.get("href", "")
                        if href.startswith("http") and valid_url(href):
                            results.append({"title": a.get_text(strip=True)[:140], "url": href, "source": "Bing"})
            except Exception as e:
                trip_circuit("bing")
                BOT_METRICS["engine_errors"]["bing"] += 1
                apply_domain_penalty("bing")
                log.warning("bing_fail", err=str(e)[:80])
            finally:
                if resp and not resp.closed: resp.close()
            return results

async def scrape_google(query: str) -> List[Dict]:
    if is_circuit_open("google"): return []
    await adaptive_domain_sleep("google")
    async with DOMAIN_LIMITS["google"]:
        async with SCRAPE_SEMAPHORE:
            results = []
            resp = None
            try:
                if GOOGLE_SEARCH_API_KEY and GOOGLE_SEARCH_CX:
                    api_url = f"https://www.googleapis.com/customsearch/v1?key={GOOGLE_SEARCH_API_KEY}&cx={GOOGLE_SEARCH_CX}&q={quote_plus(query)}&fileType=pdf"
                    api_resp = await safe_get(api_url, domain_marker="google")
                    if api_resp:
                        try:
                            async with api_resp:
                                if api_resp.status == 200:
                                    data = await api_resp.json()
                                    if "items" in data:
                                        for item in data["items"]:
                                            if valid_url(item.get("link", "")):
                                                results.append({"title": item.get("title", "")[:140], "url": item["link"], "source": "Google-API"})
                                    BOT_METRICS["api_fallback_hits"] += 1
                        except Exception:
                            if api_resp and not api_resp.closed: api_resp.close()
                            pass
                
                if len(results) >= 5:
                    return results
                
                url = "https://www.google.com/search?q=" + quote_plus(query) + "&num=20&as_filetype=pdf"
                
                google_headers = {
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept-Language": "en-US,en;q=0.9",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "same-origin",
                    "DNT": "1",
                }
                
                resp = await safe_get(url, headers=google_headers, explicit_proxy=await get_domain_aware_proxy("google"), domain_marker="google")
                if not resp:
                    apply_domain_penalty("google")
                    trip_circuit("google")
                    return results
                relieve_domain_penalty("google")
                heal_circuit("google")
                
                try:
                    async with resp:
                        html = await resp.text()
                except Exception:
                    if resp and not resp.closed: resp.close()
                    raise
                    
                if "our systems have detected unusual traffic" in html.lower():
                    apply_domain_penalty("google")
                    trip_circuit("google")
                    log.warning("google_captcha_hit")
                    return results
                soup = BeautifulSoup(html, "html.parser")

                for sel in ("yuRUbf", "tF2Cxc", "g"):
                    for i, div in enumerate(soup.find_all("div", class_=sel)):
                        if i % 10 == 0: await asyncio.sleep(0)  
                        a = div.find("a", href=True)
                        if not a:
                            continue
                        href = a["href"]
                        if href.startswith("/url?q="):
                            href = unquote(href[7:].split("&")[0])
                        if href.startswith("http") and valid_url(href):
                            h3 = div.find("h3")
                            title = h3.get_text(strip=True) if h3 else href.split("/")[-1]
                            results.append({"title": title[:140], "url": href, "source": "Google"})
                    if results:
                        break

                if not results:
                    for i, a in enumerate(soup.find_all("a", href=True)):
                        if i % 10 == 0: await asyncio.sleep(0)  
                        href = a["href"]
                        if href.startswith("/url?q="):
                            href = unquote(href[7:].split("&")[0])
                        if href.startswith("http") and ".pdf" in href.lower() and valid_url(href) and "google.com" not in href.lower():
                            results.append({"title": a.get_text(strip=True)[:140] or href, "url": href, "source": "Google"})
                
                if not results:
                    links = re.findall(r'(https?://[^\s"\'<>]+?\.pdf)', html, re.IGNORECASE)
                    for link in links:
                        if valid_url(link) and "google.com" not in link.lower():
                            results.append({"title": unquote(link.split("/")[-1])[:140], "url": link, "source": "Google-RegexFallback"})

            except Exception as e:
                trip_circuit("google")
                BOT_METRICS["engine_errors"]["google"] += 1
                apply_domain_penalty("google")
                log.warning("google_fail", err=str(e)[:80])
            finally:
                if resp and not resp.closed: resp.close()
            return results

async def scrape_telegram(keyword: str) -> List[Dict]:
    if is_circuit_open("telegram"): return []
    results = []
    kw_words = [w for w in keyword.lower().split() if len(w) > 3][:4]
    for channel in TELEGRAM_CHANNELS:
        await adaptive_domain_sleep("telegram")
        async with DOMAIN_LIMITS["telegram"]:
            async with SCRAPE_SEMAPHORE:
                resp = None
                try:
                    resp = await safe_get(f"https://t.me/s/{channel}", explicit_proxy=await get_domain_aware_proxy("telegram"), domain_marker="telegram")
                    if not resp:
                        apply_domain_penalty("telegram")
                        continue
                    relieve_domain_penalty("telegram")
                    heal_circuit("telegram")
                    try:
                        async with resp:
                            if resp.status != 200:
                                continue
                            html = await resp.text()
                    except Exception:
                        if resp and not resp.closed: resp.close()
                        raise
                    soup = BeautifulSoup(html, "html.parser")
                    for i, msg in enumerate(soup.find_all("div", class_="tgme_widget_message_wrap")):
                        if i % 10 == 0: await asyncio.sleep(0)  
                        el = msg.find("div", class_="tgme_widget_message_text")
                        if not el:
                            continue
                        text = el.get_text(" ", strip=True)
                        
                        if not match_keywords(text, kw_words):
                            continue
                            
                        a = msg.find("a", href=True)
                        link = a["href"] if a else f"https://t.me/{channel}"
                        results.append({
                            "title": f"[TG @{channel}] {text[:110]}",
                            "url": link,
                            "source": "Telegram"
                        })
                        if len(results) >= 4:
                            break
                except Exception:
                    trip_circuit("telegram")
                    BOT_METRICS["engine_errors"]["telegram"] += 1
                    apply_domain_penalty("telegram")
                    pass
                finally:
                    if resp and not resp.closed: resp.close()
            await asyncio.sleep(0.4)
    return results

# ─────────────────────────────────────────────
#  COMBINED SEARCH + DUAL CACHE
# ─────────────────────────────────────────────
async def full_search(exam_key: str, exam_label: str, region: str,
                      year: Optional[int], mat: str) -> List[Dict]:
    BOT_METRICS["total_searches"] += 1 
    
    sid = make_search_id(exam_key, region, year or 0, mat)

    cached = await redis_get(f"res:{sid}")
    if cached:
        log.info("redis_hit", sid=sid)
        return decompress(cached)

    if MONGO_DB is not None:
        try:
            doc = await MONGO_DB.search_cache.find_one({"_id": sid})
            if doc and "data" in doc:
                compressed: bytes = doc["data"]
                await redis_set(f"res:{sid}", compressed)
                return decompress(compressed)
        except Exception as e:
            log.warning("mongo_read_fail", err=str(e)[:60])

    global REDIS_DISABLED_WARNING
    if REDIS_DISABLED_WARNING:
        async with CACHE_LOCK:
            if sid in LOCAL_MEM_CACHE:
                log.info("local_mem_cache_hit", sid=sid)
                return LOCAL_MEM_CACHE[sid]
        log.info("fallback_throttle_active")
        await asyncio.sleep(1.5)

    queries = build_queries(exam_label, region, year, mat)
    log.info("live_search", exam=exam_key, region=region, year=year, mat=mat)

    tasks = []
    for q in queries[:4]:          
        tasks.append(scrape_ddg(q))
        tasks.append(scrape_bing(q))
    for q in queries[:2]:
        tasks.append(scrape_google(q))
    tasks.append(scrape_telegram(f"{exam_label} {region} pharmacist {year or ''}"))

    await asyncio.sleep(0.2)
    batch = await asyncio.gather(*tasks, return_exceptions=True)
    all_results: List[Dict] = []
    
    for b in batch:
        if isinstance(b, list):
            all_results.extend(b)
        elif isinstance(b, Exception):
            log.error("scraper_engine_dropped", err=str(b))

    seen: set = set()
    bad_words = {"admit card", "notification", "recruitment", "registration",
                 "vacancy", "apply online", "jobs", "hall ticket", "login"}
    
    rank_keywords = [w.lower() for w in exam_label.split() + region.split()]
    if year:
        rank_keywords.append(str(year))
        
    scored: List[Dict] = []
    current_year = datetime.now().year
    
    for item in all_results:
        await asyncio.sleep(0)  
        url = item.get("url", "")
        if not url or url in seen or not valid_url(url):
            continue
        seen.add(url)
        t_l  = item.get("title", "").lower()
        u_l  = url.lower()
        if any(bw in t_l for bw in bad_words) and ".pdf" not in u_l:
            continue
        score = 0
        if ".pdf" in u_l:                                           score += 10
        if any(d in u_l for d in ["gov.in","nic.in","ac.in",".edu"]): score += 6
        if "telegram" in item.get("source","").lower():             score += 3
        if year and str(year) in u_l:                               score += 4
        if "pharmacist" in u_l:                                     score += 2
        
        keyword_matches = sum(1 for w in rank_keywords if w in t_l or w in u_l)
        score += (keyword_matches * 5)
        
        for y in range(current_year - 2, current_year + 1):
            if str(y) in u_l or str(y) in t_l:
                score += 3
        
        item["score"] = score
        scored.append(item)

    scored.sort(key=lambda x: x["score"], reverse=True)
    
    final = scored[:800]
    log.info("search_done", raw=len(all_results), final=len(final))

    if final:
        compressed = compress(final)   
        
        if REDIS:
            await redis_set(f"res:{sid}", compressed, SESSION_TTL)
        
        if REDIS_DISABLED_WARNING:
            async with CACHE_LOCK:
                LOCAL_MEM_CACHE[sid] = final
                if len(LOCAL_MEM_CACHE) > 500:
                    LOCAL_MEM_CACHE.pop(next(iter(LOCAL_MEM_CACHE)))
                
        if MONGO_DB is not None:
            try:
                await MONGO_DB.search_cache.update_one(
                    {"_id": sid},
                    {"$set": {"data": compressed, "ts": datetime.utcnow()}},
                    upsert=True
                )
            except Exception as e:
                log.warning("mongo_write_fail", err=str(e)[:60])

    return final

# ─────────────────────────────────────────────
#  SESSION STORE & PERSISTENCE
# ─────────────────────────────────────────────
def soft_purge_sessions(limit: int = 2000, keep: int = 1500):
    if len(SESSION_MAP) > limit + 500:
        log.error("emergency_ram_purge_triggered")
        SESSION_MAP.clear()
        SESSION_EXPIRY.clear()
        SESSION_LAST_ACCESSED.clear()
        return

    if len(SESSION_MAP) > limit:
        sorted_keys = sorted(SESSION_LAST_ACCESSED.keys(), key=lambda k: SESSION_LAST_ACCESSED[k])
        keys_to_remove = sorted_keys[:max(0, len(SESSION_MAP) - keep)]
        for k in keys_to_remove:
            if k not in MONGO_SYNCED_SIDS:
                create_safe_task(sqlite_save(k, SESSION_MAP[k]), name=f"sqlite_save_{k[:8]}")
            SESSION_MAP.pop(k, None)
            SESSION_EXPIRY.pop(k, None)
            SESSION_LAST_ACCESSED.pop(k, None)
        log.warning("session_map_soft_purged_lru_to_disk", removed=len(keys_to_remove))

async def store_session_safe(sid: str, data: list):
    async with CACHE_LOCK:
        soft_purge_sessions(limit=2000, keep=1500)
        SESSION_EXPIRY[sid] = time.time() + SESSION_TTL
        SESSION_LAST_ACCESSED[sid] = time.time()
        SESSION_MAP[sid] = compress(data)

async def store_session(sid: str, results: List[Dict]):
    compressed = compress(results)
    await store_session_safe(sid, results)
    
    # ADDED BUG FIX 1: True Session Storage across all distributed Nodes securely
    if REDIS:
        await redis_set(f"session:{sid}", compressed, SESSION_TTL)
        
    if MONGO_DB is not None:
        try:
            await MONGO_DB.sessions.update_one(
                {"_id": sid},
                {"$set": {"data": compressed, "ts": datetime.utcnow()}},
                upsert=True
            )
            async with STATE_LOCK:
                MONGO_SYNCED_SIDS.add(sid) 
        except Exception:
            pass

async def load_session(sid: str) -> Optional[List[Dict]]:
    redis_data = await redis_get(f"session:{sid}")
    if redis_data:
        return decompress(redis_data)

    async with CACHE_LOCK:
        if sid in SESSION_EXPIRY and SESSION_EXPIRY[sid] < time.time():
            SESSION_MAP.pop(sid, None)
            SESSION_EXPIRY.pop(sid, None)
            SESSION_LAST_ACCESSED.pop(sid, None)
            
        if sid in SESSION_MAP:
            SESSION_LAST_ACCESSED[sid] = time.time() 
            return decompress(SESSION_MAP[sid])
    
    disk_data = await sqlite_load(sid)
    if disk_data:
        async with CACHE_LOCK:
            SESSION_MAP[sid] = disk_data
            SESSION_LAST_ACCESSED[sid] = time.time()
            SESSION_EXPIRY[sid] = time.time() + SESSION_TTL
        return decompress(disk_data)
        
    if MONGO_DB is not None:
        try:
            doc = await MONGO_DB.sessions.find_one({"_id": sid})
            if doc and "data" in doc:
                async with CACHE_LOCK:
                    SESSION_MAP[sid] = normalize_bytes(doc.get("data"))
                    SESSION_LAST_ACCESSED[sid] = time.time()
                async with STATE_LOCK:
                    MONGO_SYNCED_SIDS.add(sid)
                return decompress(normalize_bytes(doc.get("data")))
        except Exception:
            pass
    return None

async def restore_sessions_on_startup():
    if MONGO_DB is not None:
        try:
            time_limit = datetime.utcnow() - timedelta(hours=2)
            cursor = MONGO_DB.sessions.find({"ts": {"$gt": time_limit}}).sort("ts", -1).limit(50)
            async for doc in cursor:
                sid = doc["_id"]
                data = normalize_bytes(doc.get("data"))
                if data:
                    async with CACHE_LOCK:
                        SESSION_MAP[sid] = data
                        SESSION_EXPIRY[sid] = time.time() + SESSION_TTL
                        SESSION_LAST_ACCESSED[sid] = time.time()
                    async with STATE_LOCK:
                        MONGO_SYNCED_SIDS.add(sid)
            log.info("sessions_restored", count=len(SESSION_MAP))
        except Exception as e:
            log.warning("session_restore_failed", err=str(e)[:60])

def on_burst_worker_done(task):
    global BURST_WORKERS
    BURST_TASKS.discard(task)
    TASK_REGISTRY["burst_workers"].discard(task)
    BURST_WORKERS = max(0, BURST_WORKERS - 1)
    log.info("burst_worker_released", active_burst=BURST_WORKERS)
    
def on_static_worker_done(task):
    TASK_REGISTRY["static_workers"].discard(task)
    log.info("static_worker_died_and_cleaned")

async def session_cleanup_task():
    global BURST_WORKERS, LAST_BURST_TIME, BURST_COOLDOWN, USER_DL_COUNTS, USER_DAILY_QUOTA
    while True:
        current_qsize = DOWNLOAD_QUEUE.qsize()
        
        if DOWNLOAD_LATENCY_EMA > 10.0:
            BURST_COOLDOWN = max(10.0, BURST_COOLDOWN * 0.9) 
        else:
            BURST_COOLDOWN = min(60.0, BURST_COOLDOWN * 1.1) 
            
        needs_burst = (current_qsize >= (DOWNLOAD_QUEUE.maxsize * 0.8)) or (current_qsize > 5 and DOWNLOAD_LATENCY_EMA > 15.0)
        
        if ACTIVE_DL + BURST_WORKERS >= MAX_TOTAL_WORKERS:
            log.warning("global_worker_cap_reached_blocking_burst")
            needs_burst = False
            
        if needs_burst:
            log.warning("queue_backpressure_or_latency_high", size=current_qsize, latency=DOWNLOAD_LATENCY_EMA)
            
            if BURST_WORKERS < MAX_BURST_WORKERS and (time.time() - LAST_BURST_TIME) > BURST_COOLDOWN and EVENT_LOOP_LAG < 0.1:
                LAST_BURST_TIME = time.time()
                t = create_safe_task(download_worker(STOP_EVENT), name=f"burst_dl_{uuid.uuid4().hex[:4]}")
                BURST_TASKS.add(t)
                TASK_REGISTRY["burst_workers"].add(t)
                t.add_done_callback(on_burst_worker_done) 
                BURST_WORKERS += 1
                log.info("spawned_predictive_burst_worker", total_burst=BURST_WORKERS)
            elif EVENT_LOOP_LAG >= 0.1:
                log.warning("burst_worker_blocked_due_to_cpu_lag")
            elif (time.time() - LAST_BURST_TIME) <= BURST_COOLDOWN:
                log.info("burst_worker_cooldown_active")
            else:
                log.warning("burst_worker_cap_reached_max_load")
            
        now = time.time()
        
        async with STATE_LOCK:
            inactive_users = [k for k, v in USER_DL_COUNTS.items() if v <= 0]
            for user in inactive_users:
                USER_DL_COUNTS.pop(user, None)
                
            if len(MONGO_SYNCED_SIDS) > 5000:
                sids_to_clear = list(MONGO_SYNCED_SIDS)[:1000]
                for sid in sids_to_clear:
                    MONGO_SYNCED_SIDS.discard(sid)
                    
            stuck_users = [u for u, ts in USER_DL_TIMESTAMPS.items() if now - ts > 600]
            for u in stuck_users:
                USER_DL_COUNTS[u] = 0
                USER_DL_TIMESTAMPS.pop(u, None)
                log.warning("deadlocked_user_quota_force_reset", user_id=u)
        
        async with CACHE_LOCK:
            for k in list(SESSION_EXPIRY.keys()):
                if SESSION_EXPIRY[k] < now:
                    SESSION_EXPIRY.pop(k, None)
                    SESSION_MAP.pop(k, None)
                    SESSION_LAST_ACCESSED.pop(k, None)
                    async with STATE_LOCK:
                        MONGO_SYNCED_SIDS.discard(k)
            if len(REGION_ID_MAP) > 1000:
                REGION_ID_MAP.pop(next(iter(REGION_ID_MAP)))
                
        await save_persistent_metrics()
        await asyncio.sleep(300)

async def global_health_monitor_task():
    global LAST_BAN_CLEAR_TIME, USER_DAILY_QUOTA
    while True:
        for domain in DOMAIN_PENALTY:
            if DOMAIN_PENALTY[domain] > 0.0:
                DOMAIN_PENALTY[domain] = max(0.0, DOMAIN_PENALTY[domain] - 0.2)

        async with STATE_LOCK:
            for p in list(PROXIES):
                if p in DEAD_PROXIES:
                    if PROXY_REVIVE_COUNTS.get(p, 0) < 3:
                        DEAD_PROXIES.remove(p)
                        PROXY_FAILS[p] = 9 
                        PROXY_REVIVE_COUNTS[p] = PROXY_REVIVE_COUNTS.get(p, 0) + 1
                        log.info("proxy_revived_for_probation", proxy=p)
                    else:
                        ABSOLUTE_DEAD_PROXIES.add(p)
                elif PROXY_FAILS.get(p, 0) > 0:
                    PROXY_FAILS[p] = max(0, PROXY_FAILS[p] - 1)
                    
                if p not in DEAD_PROXIES and p in PROXY_LATENCY:
                    PROXY_LATENCY[p] = (PROXY_LATENCY[p] * 0.9) + (5.0 * 0.1)
                    
            if time.time() - LAST_BAN_CLEAR_TIME > 86400:
                GLOBAL_BANS.clear()
                USER_VIOLATIONS.clear()
                USER_DAILY_QUOTA.clear() 
                LAST_BAN_CLEAR_TIME = time.time()
                log.info("global_bans_and_quotas_daily_reset_completed")

        await asyncio.sleep(600) 

async def event_loop_monitor_task():
    global EVENT_LOOP_LAG
    while True:
        start = time.time()
        await asyncio.sleep(1.0)
        lag = time.time() - start - 1.0
        EVENT_LOOP_LAG = (EVENT_LOOP_LAG * 0.8) + (max(0, lag) * 0.2)
        
        now = time.time()
        # ADDED BUG FIX 2: Fixed Memory Leak by adding Sweeper to DNS Cache
        async with DNS_CACHE_LOCK:
            keys_to_delete = [k for k, v in DNS_CACHE.items() if now - v['ts'] > DNS_CACHE_TTL]
            for k in keys_to_delete: 
                del DNS_CACHE[k]

async def session_rotator_task():
    global SESSION
    while True:
        await asyncio.sleep(3600 * 4) 
        old_session = SESSION
        
        resolver = SSRFSenseResolver()
        connector = aiohttp.TCPConnector(
            limit=100, limit_per_host=20,
            ssl=True, 
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            resolver=resolver
        )
        SESSION = aiohttp.ClientSession(connector=connector)
        await asyncio.sleep(300) 
        if old_session and not old_session.closed:
            await old_session.close()
        log.info("aiohttp_session_rotated_safely")

async def redis_stuck_job_recovery_task():
    if not REDIS:
        return
    while True:
        await asyncio.sleep(3600) 
        try:
            for _ in range(100):
                item = await REDIS.rpoplpush("processing_dl_queue", "global_dl_queue")
                if not item:
                    break
                log.warning("redis_stuck_job_recovered", data=item[:50])
        except Exception:
            pass

async def redis_queue_puller_task():
    if not REDIS:
        return
    while True:
        try:
            item = await REDIS.brpoplpush("global_dl_queue", "processing_dl_queue", timeout=5)
            if item:
                payload = json.loads(item)
                
                url = payload.get('url')
                if not url: 
                    await REDIS.lrem("processing_dl_queue", 1, item)
                    continue
                
                job_id = hashlib.sha256(url.encode()).hexdigest()
                
                # ADDED BUG FIX: Final safety check replacing missing set lock without racing
                is_done = await REDIS.get(f"processed_job:{job_id}")
                if is_done:
                    log.info("duplicate_job_skipped_atomic", job_id=job_id)
                    await REDIS.lrem("processing_dl_queue", 1, item)
                    continue
                
                try:
                    DOWNLOAD_QUEUE.put_nowait(("app_ref", payload['chat_id'], url, payload['title'], payload['user_id']))
                except asyncio.QueueFull:
                    await REDIS.lpush("global_dl_queue", item)
                    
                await REDIS.lrem("processing_dl_queue", 1, item)
        except Exception as e:
            log.error("redis_puller_error", err=str(e))
            await asyncio.sleep(2)

# ─────────────────────────────────────────────
#  KEYBOARD BUILDERS
# ─────────────────────────────────────────────
def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(v["label"], callback_data=f"exam|{k}")]
            for k, v in EXAM_TREE.items()]
    rows.append([
        InlineKeyboardButton("🔍 Custom Search", callback_data="custom_search"),
        InlineKeyboardButton("ℹ️ Help", callback_data="help"),
    ])
    if user_id in ADMIN_IDS:
        rows.append([InlineKeyboardButton("⚙️ Admin Panel", callback_data="admin|dash")])
    return InlineKeyboardMarkup(rows)

def regions_kb(exam_key: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(f"📍 {r}", callback_data=f"region|{exam_key}|{get_region_hash(r)}")]
        for r in EXAM_TREE[exam_key]["regions"]
    ]
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)

def years_kb(exam_key: str, region_hash: str) -> InlineKeyboardMarkup:
    rows, row = [], []
    for yr in YEARS:
        row.append(InlineKeyboardButton(str(yr), callback_data=f"mattype|{exam_key}|{region_hash}|{yr}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("📅 All Years", callback_data=f"mattype|{exam_key}|{region_hash}|0")])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"exam|{exam_key}")])
    return InlineKeyboardMarkup(rows)

def mattype_kb(exam_key: str, region_hash: str, year: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(label, callback_data=f"dosearch|{exam_key}|{region_hash}|{year}|{code}|1")]
        for code, label in MATERIAL_TYPES.items()
    ]
    rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"region|{exam_key}|{region_hash}")])
    return InlineKeyboardMarkup(rows)

def results_kb(results: List[Dict], page: int,
               exam_key: str, region_hash: str, year: int, mat: str,
               sid: str) -> InlineKeyboardMarkup:
    per_page = 10
    start = (page - 1) * per_page
    sliced = results[start:start + per_page]
    src_icon = {"DDG": "🔷", "Bing": "🔶", "Google": "🔴", "Google-API": "🟢", "Telegram": "📱"}

    rows = []
    for i, item in enumerate(sliced):
        g_idx = start + i
        icon  = src_icon.get(item.get("source", ""), "🔗")
        t     = item["title"]
        short = (t[:36] + "…") if len(t) > 36 else t
        is_pdf = ".pdf" in item["url"].lower()
        rows.append([
            InlineKeyboardButton(f"{icon}[{g_idx+1}] {short}", url=item["url"]),
            InlineKeyboardButton(
                "📥 PDF" if is_pdf else "🌐 Open",
                callback_data=f"dl|{sid}|{g_idx}"
            ),
        ])

    total_pages = max(1, (len(results) - 1) // per_page + 1)
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️ Prev",
            callback_data=f"dosearch|{exam_key}|{region_hash}|{year}|{mat}|{page-1}"))
    if start + per_page < len(results):
        nav.append(InlineKeyboardButton("Next ➡️",
            callback_data=f"dosearch|{exam_key}|{region_hash}|{year}|{mat}|{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([
        InlineKeyboardButton(f"📊 {page}/{total_pages}  ({len(results)} results)", callback_data="noop"),
        InlineKeyboardButton("🔙 Menu", callback_data="back_main"),
    ])
    return InlineKeyboardMarkup(rows)

# ─────────────────────────────────────────────
#  DOWNLOAD WORKER 
# ─────────────────────────────────────────────
def safe_task_done():
    try:
        DOWNLOAD_QUEUE.task_done()
    except Exception:
        pass

def is_complete_download(size: int, cl: int) -> bool:
    if not cl:
        return True
    return size >= cl * 0.6

# ADDED BUG FIX 3: PyPDF CPU DOS Fix to restrict massive compressed PDF scans
def _cpu_bound_pdf_check(tmp_path):
    if not pypdf:
        return True
    try:
        reader = pypdf.PdfReader(tmp_path)
        if len(reader.pages) < 1:
            raise ValueError("Empty PDF")
        if len(reader.pages) > 1000:
            raise ValueError("PDF Page Count DOS Threshold Exceeded")
            
        malicious_tags = ["/AA", "/JavaScript", "/JS", "/Launch", "/EmbeddedFiles", "/SubmitForm", "/ImportData", "/OpenAction", "/ObjStm", "/Action", "/GoToE", "/RichMedia"]
        
        # Scan only first 50 pages to save CPU on massive valid files
        for page in list(reader.pages)[:50]:
            try:
                page_str = str(page.get_object()) 
                for tag in malicious_tags:
                    if tag in page_str:
                        raise ValueError(f"Advanced Embedded Malware execution detected in PDF: {tag}")
            except ValueError as ve:
                raise ve
            except Exception:
                pass 
        return True
    except Exception as e:
        raise ValueError(f"Corrupt or Malicious PDF structure: {str(e)}")

async def worker_safe_send(client, chat_id, text):
    try:
        await client.send_message(chat_id, text)
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        try:
            await client.send_message(chat_id, text)
        except Exception:
            pass
    except Exception:
        pass

async def worker_safe_doc(client, chat_id, doc_path, file_name, caption):
    try:
        await client.send_document(chat_id=chat_id, document=doc_path, file_name=file_name, caption=caption)
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        try:
            await client.send_document(chat_id=chat_id, document=doc_path, file_name=file_name, caption=caption)
        except Exception:
            pass
    except Exception:
        pass

async def download_worker(stop_event: asyncio.Event):
    global ACTIVE_DL, DOWNLOAD_LATENCY_EMA
    while not stop_event.is_set():
        try:
            task = await asyncio.wait_for(DOWNLOAD_QUEUE.get(), timeout=2)
        except asyncio.TimeoutError:
            continue
            
        # ADDED BUG FIX 6: Deadlock fix by verifying exact unpack length before failing
        if not isinstance(task, tuple) or len(task) != 5:
            safe_task_done()
            continue

        try:
            client_ref, chat_id, url, title, user_id = task
            if client_ref in ("app_ref", "restored_app"): client_ref = app 
        except Exception:
            safe_task_done()
            continue
            
        tmp_path = None   
        sent_ok  = False
        dl_start_time = time.time()

        try:
            async with DOWNLOAD_SEMAPHORE:
                async with ACTIVE_DL_LOCK:
                    ACTIVE_DL += 1
                try:
                    if not valid_url(url):
                        await worker_safe_send(client_ref, chat_id, f"❌ Unsafe URL blocked.")
                        sent_ok = True
                        continue

                    resp = await safe_get(url, timeout=aiohttp.ClientTimeout(total=90))
                    if not resp:
                        await worker_safe_send(client_ref, chat_id, f"❌ Could not reach:\n{url}")
                        sent_ok = True
                        continue

                    try:
                        async with resp:
                            ct     = resp.headers.get("Content-Type", "")
                            is_pdf = "pdf" in ct.lower() or ".pdf" in url.lower()

                            if not is_pdf:
                                await worker_safe_send(
                                    client_ref, chat_id,
                                    f"🌐 **Web Resource:**\n**{title[:200]}**\n\n🔗 {url}"
                                )
                                sent_ok = True
                                continue

                            cl = int(resp.headers.get("Content-Length", 0))
                            if cl > 50 * 1024 * 1024:
                                await worker_safe_send(
                                    client_ref, chat_id,
                                    f"⚠️ File too large ({cl//1024//1024} MB). Open link:\n{url}"
                                )
                                sent_ok = True
                                continue

                            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
                            tmp_path = tmp.name
                            tmp.close()

                            size = 0
                            complete = True
                            async with aiofiles.open(tmp_path, "wb") as f:
                                async for chunk in resp.content.iter_chunked(8192):
                                    await f.write(chunk)
                                    size += len(chunk)
                                    if size > 50 * 1024 * 1024:
                                        complete = False
                                        break

                            if not complete:
                                await worker_safe_send(
                                    client_ref, chat_id,
                                    f"⚠️ File too large to download. Open directly:\n{url}"
                                )
                                sent_ok = True
                                continue

                            if cl and not is_complete_download(size, cl):
                                await worker_safe_send(
                                    client_ref, chat_id,
                                    f"⚠️ Incomplete download ({size//1024}KB of {cl//1024}KB):\n{url}"
                                )
                                sent_ok = True
                                continue
                                
                            async with aiofiles.open(tmp_path, "rb") as f_check:
                                magic_bytes = await f_check.read(4)
                                try:
                                    # ADDED BUG FIX 8: Proper Strict EOF Validation logic checking the correct end slice
                                    await f_check.seek(max(0, size - 128))
                                    eof_bytes = await f_check.read()
                                except Exception:
                                    eof_bytes = b""
                                if magic_bytes != b"%PDF" or (b"%%EOF" not in eof_bytes and b"%EOF" not in eof_bytes):
                                    await worker_safe_send(client_ref, chat_id, f"⚠️ Security Block: Invalid PDF file signature:\n{url}")
                                    sent_ok = True
                                    continue
                                    
                            if pypdf:
                                try:
                                    await asyncio.wait_for(asyncio.to_thread(_cpu_bound_pdf_check, tmp_path), timeout=5.0)
                                except Exception as e:
                                    log.warning("pypdf_validation_failed", url=url, err=str(e))
                                    await worker_safe_send(client_ref, chat_id, f"⚠️ Security Block: Corrupt or deeply obfuscated PDF payload blocked:\n{url}")
                                    sent_ok = True
                                    continue
                    except Exception:
                        if resp and not resp.closed: resp.close()
                        raise

                    safe_name = re.sub(r"[^\w\s\-]", "", title)[:60].strip() + ".pdf"
                    
                    await worker_safe_doc(
                        client_ref, chat_id, tmp_path, safe_name,
                        f"📄 **{title[:180]}**\n📦 {size // 1024} KB\n🔗 {url[:100]}"
                    )
                    sent_ok = True
                    log.info("pdf_sent", size_kb=size // 1024, title=title[:60])
                    
                    BOT_METRICS["pdfs_downloaded"] += 1
                    BOT_METRICS["bytes_downloaded"] += size
                    
                    dl_latency = time.time() - dl_start_time
                    DOWNLOAD_LATENCY_EMA = (DOWNLOAD_LATENCY_EMA * 0.8) + (dl_latency * 0.2)
                    
                    # Store Redis job lock ONLY AFTER a fully successful delivery to chat
                    if REDIS and url:
                        try:
                            job_hash = hashlib.sha256(url.encode()).hexdigest()
                            await REDIS.set(f"processed_job:{job_hash}", "1", ex=86400)
                        except Exception:
                            pass

                finally:
                    async with ACTIVE_DL_LOCK:
                        ACTIVE_DL = max(0, ACTIVE_DL - 1)

        except asyncio.CancelledError:
            log.warning("download_cancelled_mid_flight", url=url)
            sent_ok = True
            raise
        except Exception as e:
            log.error("dl_crash", err=str(e)[:100], url=url)
            if not sent_ok:
                try:
                    await worker_safe_send(
                        client_ref, chat_id,
                        f"❌ Download failed:\n**{title[:150]}**\n🔗 {url}\n`{str(e)[:80]}`"
                    )
                except Exception:
                    pass
        finally:
            async with STATE_LOCK:
                USER_DL_COUNTS[user_id] = max(0, USER_DL_COUNTS.get(user_id, 1) - 1)
                USER_DL_TIMESTAMPS.pop(user_id, None)
            safe_task_done()
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

# ─────────────────────────────────────────────
#  COMMAND HANDLERS
# ─────────────────────────────────────────────
@app.on_message(filters.command("start") & filters.private)
@safe
async def cmd_start(_, msg: Message):
    if ALLOWED_USERS and msg.from_user.id not in ALLOWED_USERS and msg.from_user.id not in ADMIN_IDS:
        return await msg.reply_text("❌ This is a private pharmacist bot.")
        
    if await is_rate_limited(msg.from_user.id, "start", 5):
        return await msg.reply_text("⏳ Please wait a moment. Anti-Spam protection active.")
    name = msg.from_user.first_name or "Friend"
    await msg.reply_text(
        f"👋 **Welcome, {name}!**\n\n"
        "🎯 **Pharmacist PYQ & Study Material Bot v18.0 Absolute Titan Core**\n\n"
        "✅ AIIMS CRE (14 institutes) | ESIC | DSSSB | NHM\n"
        "✅ Drug Inspector | GPAT | State PSC (9 boards)\n"
        "✅ PYQ | Syllabus | Notes | Answer Keys | Mock | Books\n\n"
        "🔍 Google + Bing + DDG + Telegram Channels\n\n"
        "👇 Select your exam:",
        reply_markup=main_menu_kb(msg.from_user.id)
    )

@app.on_message(filters.command("stats") & filters.private)
@safe
async def cmd_stats(_, msg: Message):
    up = int(time.time() - BOT_START_TIME)
    h, r = divmod(up, 3600)
    m, s = divmod(r, 60)
    
    async with CACHE_LOCK:
        session_len = len(SESSION_MAP)
        
    await msg.reply_text(
        f"📊 **Bot Architecture Stats**\n\n"
        f"⏱ Uptime: `{h}h {m}m {s}s`\n"
        f"📥 Active Downloads: `{ACTIVE_DL}`\n"
        f"📋 Local Queue: `{DOWNLOAD_QUEUE.qsize()}` / {DOWNLOAD_QUEUE.maxsize}\n"
        f"💾 Active LRU Sessions: `{session_len}`\n"
        f"🔵 Queue Status: `{'PAUSED' if QUEUE_PAUSED else 'ACTIVE'}`\n"
        f"🌐 Dead Proxies Auto-Pruned: `{len(DEAD_PROXIES)}`\n"
        f"⚡ CPU Loop Lag: `{EVENT_LOOP_LAG:.3f}s`\n"
        f"🚀 Burst Workers: `{BURST_WORKERS}` (Predictive Latency: `{DOWNLOAD_LATENCY_EMA:.1f}s`)\n"
        f"🔗 Node ID: `{NODE_ID}`\n\n"
        f"📈 Total Searches: `{BOT_METRICS['total_searches']}`\n"
        f"📄 PDFs Sent: `{BOT_METRICS['pdfs_downloaded']}`\n"
        f"🗂️ Data Processed: `{BOT_METRICS['bytes_downloaded'] // (1024*1024)} MB`"
    )

@app.on_message(filters.command("metrics") & filters.private)
@safe
async def cmd_metrics(_, msg: Message):
    if msg.from_user.id not in ADMIN_IDS:
        return await msg.reply_text("❌ Unauthorized")
    
    m_text = (
        f"📡 **Prometheus-Lite Metrics:**\n\n"
        f"🔹 **Engine Errors:**\n"
        f"   Google: `{BOT_METRICS['engine_errors']['google']}`\n"
        f"   Bing: `{BOT_METRICS['engine_errors']['bing']}`\n"
        f"   DDG: `{BOT_METRICS['engine_errors']['ddg']}`\n"
        f"   Telegram: `{BOT_METRICS['engine_errors']['telegram']}`\n\n"
        f"🔹 **Circuit Breakers (Trips):**\n"
        f"   Google: `{DOMAIN_CIRCUIT_BREAKER['google']}`\n"
        f"   Bing: `{DOMAIN_CIRCUIT_BREAKER['bing']}`\n"
        f"   DDG: `{DOMAIN_CIRCUIT_BREAKER['ddg']}`\n"
        f"   Telegram: `{DOMAIN_CIRCUIT_BREAKER['telegram']}`\n\n"
        f"🔹 **Google API Fallback Hits:** `{BOT_METRICS['api_fallback_hits']}`\n"
        f"🔹 **Synced SIDs to Mongo:** `{len(MONGO_SYNCED_SIDS)}`\n"
        f"🔹 **Proxy Pool Size:** `{len(PROXIES)}` (Dead: `{len(DEAD_PROXIES)}`)\n"
        f"🔹 **Global Spammers Banned:** `{len(GLOBAL_BANS)}`"
    )
    await msg.reply_text(m_text)

@app.on_message(filters.command("search") & filters.private)
@safe
async def cmd_search(client, msg: Message):
    query = msg.text.replace("/search", "").strip()
    if not query:
        return await msg.reply_text(
            "📝 **Usage:** `/search AIIMS pharmacist 2023 pdf`\n\n"
            "• `/search Drug Inspector UP 2022 paper`\n"
            "• `/search GPAT previous year solved`"
        )
    wait = await msg.reply_text(f"🔍 Searching: `{query}`…")
    tasks = [scrape_ddg(query), scrape_bing(query), scrape_google(query)]
    batch = await asyncio.gather(*tasks, return_exceptions=True)
    results: List[Dict] = []
    seen: set = set()
    for b in batch:
        if isinstance(b, list):
            for item in b:
                if item["url"] not in seen and valid_url(item["url"]):
                    seen.add(item["url"])
                    results.append(item)
    results = results[:300]
    if not results:
        return await wait.edit_text("❌ No results found. Try different keywords.")

    q_hash = hashlib.sha256(query.encode()).hexdigest()
    if REDIS:
        if await REDIS.exists(f"active_search:{q_hash}"):
            return await wait.edit_text("⏳ Your previous identical search is still processing.")
        await REDIS.setex(f"active_search:{q_hash}", 10, "1")

    sid = make_search_id_v2("CUSTOM", query[:40], 0, "pyq", msg.from_user.id)
    await store_session(sid, results)
    await wait.edit_text(
        f"✅ Found **{len(results)}** results for `{query}`\n\n"
        "🔷 DDG  🔶 Bing  🔴 Google\n"
        "Tap title → open | 📥 → download",
        reply_markup=results_kb(results, 1, "CUSTOM", "CUSTOM", 0, "pyq", sid),
        disable_web_page_preview=True
    )

# ─────────────────────────────────────────────
#  CALLBACK ROUTER
# ─────────────────────────────────────────────
@app.on_callback_query()
@safe
async def callback_router(client, cq: CallbackQuery):
    data = cq.data

    if data == "noop":
        return await cq.answer()

    if data == "back_main":
        return await cq.message.edit_text(
            "👇 **Select your exam:**",
            reply_markup=main_menu_kb(cq.from_user.id)
        )

    if data == "help":
        return await cq.message.edit_text(
            "📖 **How to Use**\n\n"
            "1️⃣ Select exam → Region → Year → Material type\n"
            "2️⃣ Browse results, tap title to open\n"
            "3️⃣ Tap 📥 to download PDF directly to chat\n\n"
            "🔍 `/search` for custom keyword search\n"
            "📊 `/stats` for bot status\n\n"
            "🔷 DDG  🔶 Bing  🔴 Google  📱 Telegram",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Menu", callback_data="back_main")]
            ])
        )

    if data == "custom_search":
        return await cq.message.edit_text(
            "🔍 **Custom Search**\n\nSend: `/search <keywords>`\n\n"
            "• `/search AIIMS pharmacist 2022 pdf`\n"
            "• `/search Drug Inspector MP question paper`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
            ])
        )

    if data.startswith("exam|"):
        exam_key = data.split("|", 1)[1]
        info = EXAM_TREE.get(exam_key)
        if not info:
            return await cq.answer("Unknown exam", show_alert=True)
        return await cq.message.edit_text(
            f"{info['label']}\n\n📍 Select Region:",
            reply_markup=regions_kb(exam_key)
        )

    if data.startswith("region|"):
        _, exam_key, region_hash = data.split("|", 2)
        info = EXAM_TREE.get(exam_key, {})
        region_str = REGION_ID_MAP.get(region_hash, region_hash)
        return await cq.message.edit_text(
            f"{info.get('label','')}\n📍 {region_str}\n\n📅 Select Year:",
            reply_markup=years_kb(exam_key, region_hash)
        )

    if data.startswith("mattype|"):
        _, exam_key, region_hash, year_str = data.split("|", 3)
        year = int(year_str)
        info = EXAM_TREE.get(exam_key, {})
        region_str = REGION_ID_MAP.get(region_hash, region_hash)
        yr_label = str(year) if year else "All Years"
        return await cq.message.edit_text(
            f"{info.get('label','')}\n📍 {region_str} | 📅 {yr_label}\n\n📂 Select Material Type:",
            reply_markup=mattype_kb(exam_key, region_hash, year)
        )

    if data.startswith("dosearch|"):
        parts = data.split("|")
        _, exam_key, region_hash, year_str, mat, page_str = parts[:6]
        year     = int(year_str)
        page     = int(page_str)
        info     = EXAM_TREE.get(exam_key, {})
        exam_label = info.get("label", exam_key)
        region_str = REGION_ID_MAP.get(region_hash, region_hash)
        yr_label   = str(year) if year else "All Years"

        sid = make_search_id(exam_key, region_str, year, mat)

        results = await load_session(sid)
        if not results:
            try:
                await cq.message.edit_text(
                    f"🔍 **Searching…**\n\n"
                    f"{exam_label}\n📍 {region_str} | 📅 {yr_label}\n"
                    f"📂 {MATERIAL_TYPES.get(mat,'')}\n\n"
                    "⏳ Scanning Google + Bing + DDG + Telegram…"
                )
            except MessageNotModified:
                pass

            results = await full_search(exam_key, exam_label, region_str, year or None, mat)
            if not results:
                return await cq.message.edit_text(
                    "❌ No results found. Try different year or material type.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Menu", callback_data="back_main")]
                    ])
                )
            await store_session(sid, results)

        return await cq.message.edit_text(
            f"📄 **{exam_label} | {region_str}**\n"
            f"📅 {yr_label} | {MATERIAL_TYPES.get(mat,'')}\n\n"
            f"✅ **{len(results)}** resources found",
            reply_markup=results_kb(results, page, exam_key, region_hash, year, mat, sid),
            disable_web_page_preview=True
        )

    if data.startswith("dl|"):
        if QUEUE_PAUSED:
            return await cq.answer("⏸️ Downloads paused. Try later.", show_alert=True)
        _, sid, idx_str = data.split("|", 2)
        idx = int(idx_str)

        results = await load_session(sid)
        if not results:
            return await cq.answer("❌ Session expired. Search again.", show_alert=True)
        if idx >= len(results):
            return await cq.answer("❌ Item not found.", show_alert=True)
            
        async with STATE_LOCK:
            if USER_DAILY_QUOTA.get(cq.from_user.id, 0) >= 50:
                return await cq.answer("🚨 Daily limit of 50 downloads reached.", show_alert=True)
            USER_DAILY_QUOTA[cq.from_user.id] = USER_DAILY_QUOTA.get(cq.from_user.id, 0) + 1
            
            if USER_DL_COUNTS.get(cq.from_user.id, 0) >= 3:
                USER_DAILY_QUOTA[cq.from_user.id] -= 1
                return await cq.answer("🚨 Download quota reached! Please wait for current files to finish.", show_alert=True)
            USER_DL_COUNTS[cq.from_user.id] = USER_DL_COUNTS.get(cq.from_user.id, 0) + 1
            USER_DL_TIMESTAMPS[cq.from_user.id] = time.time()
        
        if REDIS:
            q_len = await REDIS.llen("global_dl_queue")
            if q_len > 1000:
                async with STATE_LOCK:
                    USER_DL_COUNTS[cq.from_user.id] = max(0, USER_DL_COUNTS.get(cq.from_user.id, 1) - 1)
                return await cq.answer("🚨 Global cluster overloaded. Try again later.", show_alert=True)

        if await is_rate_limited(cq.from_user.id, "dl", 8):
            async with STATE_LOCK:
                USER_DL_COUNTS[cq.from_user.id] = max(0, USER_DL_COUNTS.get(cq.from_user.id, 1) - 1)
            return await cq.answer("⏳ Rate Limited! Wait before next download.", show_alert=True)

        item = results[idx]
        
        payload = {
            "chat_id": cq.message.chat.id,
            "url": item["url"],
            "title": item["title"],
            "user_id": cq.from_user.id
        }
        
        if REDIS:
            # Removed premature duplicate lock here, now correctly placed in download_worker success phase
            await REDIS.lpush("global_dl_queue", json.dumps(payload))
            await cq.answer("✅ Sent to Global Cluster! Pos ~" + str(await REDIS.llen("global_dl_queue")))
        else:
            try:
                DOWNLOAD_QUEUE.put_nowait((client, cq.message.chat.id, item["url"], item["title"], cq.from_user.id))
                await cq.answer(f"✅ Queued Locally! Position ~{DOWNLOAD_QUEUE.qsize()}")
            except asyncio.QueueFull:
                async with STATE_LOCK:
                    USER_DL_COUNTS[cq.from_user.id] = max(0, USER_DL_COUNTS.get(cq.from_user.id, 1) - 1)
                await cq.answer("🚨 System Overloaded! Queue full. Please try again later.", show_alert=True)
        return

    if data.startswith("admin|"):
        if cq.from_user.id not in ADMIN_IDS:
            return await cq.answer("❌ Unauthorized", show_alert=True)
        cmd = data.split("|", 1)[1]

        if cmd == "dash":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "⏸️ Pause Queue" if not QUEUE_PAUSED else "▶️ Resume Queue",
                    callback_data="admin|toggle"
                )],
                [InlineKeyboardButton("🗑️ Clear Session Cache", callback_data="admin|clearcache")],
                [InlineKeyboardButton("🔙 Menu", callback_data="back_main")],
            ])
            async with CACHE_LOCK:
                sess_len = len(SESSION_MAP)
            return await cq.message.edit_text(
                f"⚙️ **Admin Panel**\n\n"
                f"📥 Active DL: `{ACTIVE_DL}`\n"
                f"📋 Queue: `{DOWNLOAD_QUEUE.qsize()}`\n"
                f"💾 Sessions: `{sess_len}`\n"
                f"🔵 Queue: `{'PAUSED' if QUEUE_PAUSED else 'ACTIVE'}`",
                reply_markup=kb
            )

        if cmd == "toggle":
            globals()["QUEUE_PAUSED"] = not QUEUE_PAUSED
            state = "PAUSED ⏸️" if globals()["QUEUE_PAUSED"] else "ACTIVE ▶️"
            await cq.answer(f"Queue → {state}", show_alert=True)
            return await cq.message.edit_text(
                f"Queue is now **{state}**",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Admin", callback_data="admin|dash")]
                ])
            )

        if cmd == "clearcache":
            async with CACHE_LOCK:
                SESSION_MAP.clear()
            return await cq.answer("✅ Session cache cleared.", show_alert=True)

        return

    await cq.answer("Unknown action", show_alert=True)

# ─────────────────────────────────────────────
#  LIFECYCLE
# ─────────────────────────────────────────────
STOP_EVENT        = asyncio.Event()
WORKER_TASK_LIST  = []

async def start_services():
    global SESSION, REDIS, MONGO_DB, REDIS_DISABLED_WARNING

    db_name = urlparse(MONGO_URI).path.lstrip("/") or "pharma_bot"
    
    await init_sqlite_spillover()
    await load_persistent_metrics()

    resolver = SSRFSenseResolver()
    connector = aiohttp.TCPConnector(
        limit=100, limit_per_host=20,
        ssl=True,
        ttl_dns_cache=300, 
        enable_cleanup_closed=True,
        resolver=resolver
    )
    SESSION = aiohttp.ClientSession(connector=connector)

    try:
        REDIS = aioredis.from_url(REDIS_URL, decode_responses=False)
        await REDIS.ping()
        log.info("redis_ok")
    except Exception as e:
        REDIS_DISABLED_WARNING = True
        log.warning("redis_skip", err=str(e)[:60])
        log.error("CACHE_DISABLED_MODE_ACTIVE")
        REDIS = None

    try:
        client_db = AsyncIOMotorClient(
            MONGO_URI, maxPoolSize=20, minPoolSize=2,
            serverSelectionTimeoutMS=4000
        )
        MONGO_DB = client_db[db_name]   
        await MONGO_DB.command("ping")
        try:
            await MONGO_DB.search_cache.create_index("ts", expireAfterSeconds=86400)
            await MONGO_DB.sessions.create_index("ts", expireAfterSeconds=7200)
        except Exception:
            pass
        log.info("mongo_ok", db=db_name)
    except Exception as e:
        log.warning("mongo_skip", err=str(e)[:60])
        MONGO_DB = None

    await restore_sessions_on_startup()
    load_local_queue()

    for i in range(6):
        t = create_safe_task(download_worker(STOP_EVENT), name=f"dl_{i}")
        t.add_done_callback(on_static_worker_done)
        WORKER_TASK_LIST.append(t)
        TASK_REGISTRY["static_workers"].add(t)
    log.info("workers_started", n=6)

    ct = create_safe_task(session_cleanup_task(), name="session_cleanup")
    hm = create_safe_task(global_health_monitor_task(), name="health_monitor")
    el = create_safe_task(event_loop_monitor_task(), name="event_loop_monitor")
    sr = create_safe_task(session_rotator_task(), name="session_rotator")
    rp = create_safe_task(redis_queue_puller_task(), name="redis_puller")
    rj = create_safe_task(redis_stuck_job_recovery_task(), name="redis_stuck_job_recovery")
    
    TASK_REGISTRY["background_loops"].add(ct)
    TASK_REGISTRY["background_loops"].add(hm)
    TASK_REGISTRY["background_loops"].add(el)
    TASK_REGISTRY["background_loops"].add(sr)
    TASK_REGISTRY["background_loops"].add(rp)
    TASK_REGISTRY["background_loops"].add(rj)

async def stop_services():
    STOP_EVENT.set()
    
    for t in WORKER_TASK_LIST:
        t.cancel()
    await asyncio.gather(*WORKER_TASK_LIST, return_exceptions=True)
    
    for t in list(BURST_TASKS):
        t.cancel()
    if BURST_TASKS:
        await asyncio.gather(*BURST_TASKS, return_exceptions=True)
        
    for t in TASK_REGISTRY["background_loops"]:
        t.cancel()
    
    if TASK_REGISTRY["background_loops"]:
        await asyncio.gather(*TASK_REGISTRY["background_loops"], return_exceptions=True)
        
    dump_local_queue()
        
    if SESSION and not SESSION.closed:
        await SESSION.close()
    if REDIS:
        await REDIS.aclose()
    log.info("shutdown_clean")

# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────
async def main():
    if not all([API_ID, API_HASH, BOT_TOKEN]):
        print("❌ Missing API_ID / API_HASH / BOT_TOKEN in .env")
        sys.exit(1)

    await start_services()
    print(f"🚀 Pharma Bot v22.0 Absolute Final Masterpiece [{NODE_ID}] starting…")
    try:
        await app.start()
        me = await app.get_me()
        print(f"✅ Online: @{me.username}")
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("🛑 Shutting down…")
    finally:
        await stop_services()
        try:
            await app.stop()
        except Exception:
            pass
        print("👋 Stopped cleanly.")

if __name__ == "__main__":
    asyncio.run(main())
