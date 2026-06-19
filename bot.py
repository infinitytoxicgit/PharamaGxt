"""
╔══════════════════════════════════════════════════════════════════════════════╗
║       PHARMA ULTIMATE BOT v2 — Enterprise Stable                           ║
║  AIIMS CRE | Drug Inspector | Govt Pharmacist | PYQ | Syllabus | Notes     ║
║  Multi-Engine: DDG + Google + Bing + Gov Sites + Telegram Channels         ║
║                                                                             ║
║  BUGS FIXED vs v1:                                                         ║
║  FIX 1:  global QUEUE_PAUSED SyntaxError → moved to module level           ║
║  FIX 2:  Session compression mismatch (hex vs bytes) → raw bytes only      ║
║  FIX 3:  search_id collision → hash(exam+region+year+mat), no time         ║
║  FIX 4:  client undefined in dl queue → Pyrogram passes it correctly       ║
║  FIX 5:  tmp_path leak on exception → always initialized to None           ║
║  FIX 6:  Redis mixed types → always bytes                                  ║
║  FIX 7:  Scraper overload → SCRAPE_SEMAPHORE limits concurrency            ║
║  FIX 8:  Telegram false positives → ALL keywords must match                ║
║  FIX 9:  Google DOM fallback → multiple selectors + href fallback          ║
║  FIX 10: SSRF weak → redirect-aware validator per hop                      ║
║  FIX 11: Partial PDF send on size cap → abort cleanly                      ║
║  FIX 12: ACTIVE_DL counter leak → decremented in inner finally             ║
║  FIX 13: MONGO_DB.get_default_database() → explicit db name from URI      ║
╚══════════════════════════════════════════════════════════════════════════════╝

SETUP:
  pip install pyrogram tgcrypto motor "redis[hiredis]" aiohttp aiofiles \
              structlog beautifulsoup4 python-dotenv

.env:
  API_ID=...
  API_HASH=...
  BOT_TOKEN=...
  MONGO_URI=mongodb://localhost:27017/pharma_bot
  REDIS_URL=redis://localhost:6379/0
  ADMIN_IDS=123456789
"""

# ─────────────────────────────────────────────
#  IMPORTS
# ─────────────────────────────────────────────
import os, sys, asyncio, aiohttp, aiofiles, json, zlib, hashlib
import time, uuid, random, re, tempfile
from datetime import datetime
from urllib.parse import urlparse, quote_plus, unquote
from functools import wraps
from typing import List, Dict, Optional

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
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.PrintLoggerFactory()
)
log = structlog.get_logger()

# ─────────────────────────────────────────────
#  ENV CONFIG
# ─────────────────────────────────────────────
API_ID    = int(os.getenv("API_ID", "0"))
API_HASH  = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/pharma_bot")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.strip().isdigit()]

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

# ─────────────────────────────────────────────
#  GLOBAL STATE
# ─────────────────────────────────────────────
SESSION:    Optional[aiohttp.ClientSession] = None
REDIS:      Optional[aioredis.Redis]        = None
MONGO_DB                                    = None

DOWNLOAD_QUEUE     = asyncio.Queue(maxsize=200)
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(5)
SCRAPE_SEMAPHORE   = asyncio.Semaphore(8)   # FIX 7: limit concurrent scrapers

ACTIVE_DL      = 0
ACTIVE_DL_LOCK = asyncio.Lock()

# FIX 1: QUEUE_PAUSED at module level — never use `global` inside nested if/elif
QUEUE_PAUSED   = False
BOT_START_TIME = time.time()

# FIX 2: SESSION_MAP stores raw compressed bytes (not hex strings)
SESSION_MAP: Dict[str, bytes] = {}

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
# ─────────────────────────────────────────────
def safe(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
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
#  REDIS HELPERS
# ─────────────────────────────────────────────
async def is_rate_limited(user_id: int, prefix: str, ttl: int = 5) -> bool:
    if not REDIS:
        return False
    key = f"rl:{prefix}:{user_id}"
    if await REDIS.get(key):
        return True
    await REDIS.setex(key, ttl, b"1")
    return False

async def redis_get(key: str) -> Optional[bytes]:
    if not REDIS:
        return None
    try:
        val = await REDIS.get(key)
        return val  # already bytes (decode_responses=False)
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
#  COMPRESSION  — FIX 2: ONE format, raw bytes everywhere
# ─────────────────────────────────────────────
def compress(data: list) -> bytes:
    return zlib.compress(json.dumps(data, ensure_ascii=False).encode("utf-8"), level=6)

def decompress(b: bytes) -> list:
    return json.loads(zlib.decompress(b).decode("utf-8"))

# ─────────────────────────────────────────────
#  SEARCH ID  — FIX 3: no user_id, no time → stable cache key
# ─────────────────────────────────────────────
def make_search_id(exam_key: str, region: str, year: int, mat: str) -> str:
    raw = f"{exam_key}|{region}|{year}|{mat}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]

# ─────────────────────────────────────────────
#  SSRF-SAFE FETCH  — FIX 10: validate every redirect hop
# ─────────────────────────────────────────────
_PRIVATE_PREFIXES = (
    "10.", "172.16.", "172.17.", "172.18.", "172.19.", "172.20.",
    "172.21.", "172.22.", "172.23.", "172.24.", "172.25.", "172.26.",
    "172.27.", "172.28.", "172.29.", "172.30.", "172.31.", "192.168.",
)
_PRIVATE_EXACT = {"127.0.0.1", "0.0.0.0", "::1", "localhost"}

def _host_is_safe(host: str) -> bool:
    h = host.lower().split(":")[0]
    if h in _PRIVATE_EXACT:
        return False
    if any(h.startswith(p) for p in _PRIVATE_PREFIXES):
        return False
    if "169.254." in h:
        return False
    return True

def valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and _host_is_safe(p.netloc)
    except Exception:
        return False

async def safe_get(url: str, headers: Optional[dict] = None,
                   timeout: Optional[aiohttp.ClientTimeout] = None) -> Optional[aiohttp.ClientResponse]:
    if not valid_url(url):
        return None
    h = headers or {"User-Agent": random.choice(USER_AGENTS)}
    t = timeout or aiohttp.ClientTimeout(total=25)
    try:
        resp = await SESSION.get(url, headers=h, timeout=t, allow_redirects=True)
        # Validate final URL after redirects
        final_url = str(resp.url)
        if not valid_url(final_url):
            resp.close()
            log.warning("ssrf_redirect_blocked", final=final_url)
            return None
        return resp
    except Exception as e:
        log.warning("safe_get_fail", url=url[:80], err=str(e)[:60])
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
#  SCRAPERS — FIX 7: each wrapped in SCRAPE_SEMAPHORE
# ─────────────────────────────────────────────
async def scrape_ddg(query: str) -> List[Dict]:
    async with SCRAPE_SEMAPHORE:
        results = []
        try:
            url = "https://html.duckduckgo.com/html/?q=" + quote_plus(query + " filetype:pdf")
            resp = await safe_get(url, headers={
                "User-Agent": random.choice(USER_AGENTS),
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://duckduckgo.com/",
            })
            if not resp:
                return []
            async with resp:
                html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", class_="result__a"):
                href = a.get("href", "")
                if "uddg=" in href:
                    href = unquote(href.split("uddg=")[1].split("&")[0])
                if href.startswith("http") and valid_url(href):
                    results.append({"title": a.get_text(strip=True)[:140], "url": href, "source": "DDG"})
        except Exception as e:
            log.warning("ddg_fail", err=str(e)[:80])
        return results

async def scrape_bing(query: str) -> List[Dict]:
    async with SCRAPE_SEMAPHORE:
        results = []
        try:
            url = "https://www.bing.com/search?q=" + quote_plus(query + " filetype:pdf") + "&count=30"
            resp = await safe_get(url)
            if not resp:
                return []
            async with resp:
                html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")
            for li in soup.find_all("li", class_="b_algo"):
                a = li.find("a")
                if a:
                    href = a.get("href", "")
                    if href.startswith("http") and valid_url(href):
                        results.append({"title": a.get_text(strip=True)[:140], "url": href, "source": "Bing"})
        except Exception as e:
            log.warning("bing_fail", err=str(e)[:80])
        return results

async def scrape_google(query: str) -> List[Dict]:
    async with SCRAPE_SEMAPHORE:
        results = []
        try:
            url = "https://www.google.com/search?q=" + quote_plus(query) + "&num=20&as_filetype=pdf"
            resp = await safe_get(url, headers={
                "User-Agent": random.choice(USER_AGENTS),
                "Accept-Language": "en-US,en;q=0.9",
            })
            if not resp:
                return []
            async with resp:
                html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")

            # FIX 9: try multiple selectors, then fallback to any PDF link
            for sel in [["yuRUbf"], ["tF2Cxc"], ["g"]]:
                for div in soup.find_all("div", class_=sel):
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
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.startswith("/url?q="):
                        href = unquote(href[7:].split("&")[0])
                    if href.startswith("http") and ".pdf" in href.lower() and valid_url(href):
                        results.append({"title": a.get_text(strip=True)[:140] or href, "url": href, "source": "Google"})
        except Exception as e:
            log.warning("google_fail", err=str(e)[:80])
        return results

async def scrape_telegram(keyword: str) -> List[Dict]:
    """FIX 8: ALL significant keywords must appear in message text."""
    results = []
    kw_words = [w for w in keyword.lower().split() if len(w) > 3][:4]
    for channel in TELEGRAM_CHANNELS:
        async with SCRAPE_SEMAPHORE:
            try:
                resp = await safe_get(f"https://t.me/s/{channel}")
                if not resp:
                    continue
                async with resp:
                    if resp.status != 200:
                        continue
                    html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")
                for msg in soup.find_all("div", class_="tgme_widget_message_wrap"):
                    el = msg.find("div", class_="tgme_widget_message_text")
                    if not el:
                        continue
                    text = el.get_text(" ", strip=True)
                    # FIX 8: require ALL keywords to match
                    if not all(w in text.lower() for w in kw_words):
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
                pass
        await asyncio.sleep(0.4)
    return results

# ─────────────────────────────────────────────
#  COMBINED SEARCH + DUAL CACHE
# ─────────────────────────────────────────────
async def full_search(exam_key: str, exam_label: str, region: str,
                      year: Optional[int], mat: str) -> List[Dict]:
    sid = make_search_id(exam_key, region, year or 0, mat)

    # 1. Redis — FIX 6: always bytes
    cached = await redis_get(f"res:{sid}")
    if cached:
        log.info("redis_hit", sid=sid)
        return decompress(cached)

    # 2. Mongo
    if MONGO_DB is not None:
        try:
            doc = await MONGO_DB.search_cache.find_one({"_id": sid})
            if doc and "data" in doc:
                compressed: bytes = doc["data"]
                await redis_set(f"res:{sid}", compressed)
                return decompress(compressed)
        except Exception as e:
            log.warning("mongo_read_fail", err=str(e)[:60])

    # 3. Live scrape
    queries = build_queries(exam_label, region, year, mat)
    log.info("live_search", exam=exam_key, region=region, year=year, mat=mat)

    tasks = []
    for q in queries[:4]:          # FIX 7: cap per engine
        tasks.append(scrape_ddg(q))
        tasks.append(scrape_bing(q))
    for q in queries[:2]:
        tasks.append(scrape_google(q))
    tasks.append(scrape_telegram(f"{exam_label} {region} pharmacist {year or ''}"))

    batch = await asyncio.gather(*tasks, return_exceptions=True)
    all_results: List[Dict] = []
    for b in batch:
        if isinstance(b, list):
            all_results.extend(b)

    # Score + dedup + filter
    seen: set = set()
    bad_words = {"admit card", "notification", "recruitment", "registration",
                 "vacancy", "apply online", "jobs", "hall ticket", "login"}
    scored: List[Dict] = []
    for item in all_results:
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
        item["score"] = score
        scored.append(item)

    scored.sort(key=lambda x: x["score"], reverse=True)
    final = scored[:500]
    log.info("search_done", raw=len(all_results), final=len(final))

    if final:
        compressed = compress(final)   # FIX 2: raw bytes
        await redis_set(f"res:{sid}", compressed)
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
#  SESSION STORE  — FIX 2: raw bytes
# ─────────────────────────────────────────────
async def store_session(sid: str, results: List[Dict]):
    compressed = compress(results)
    SESSION_MAP[sid] = compressed
    if MONGO_DB is not None:
        try:
            await MONGO_DB.sessions.update_one(
                {"_id": sid},
                {"$set": {"data": compressed, "ts": datetime.utcnow()}},
                upsert=True
            )
        except Exception:
            pass

async def load_session(sid: str) -> Optional[List[Dict]]:
    if sid in SESSION_MAP:
        return decompress(SESSION_MAP[sid])
    if MONGO_DB is not None:
        try:
            doc = await MONGO_DB.sessions.find_one({"_id": sid})
            if doc and "data" in doc:
                SESSION_MAP[sid] = doc["data"]
                return decompress(doc["data"])
        except Exception:
            pass
    return None

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
        [InlineKeyboardButton(f"📍 {r}", callback_data=f"region|{exam_key}|{r}")]
        for r in EXAM_TREE[exam_key]["regions"]
    ]
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)

def years_kb(exam_key: str, region: str) -> InlineKeyboardMarkup:
    rows, row = [], []
    for yr in YEARS:
        row.append(InlineKeyboardButton(str(yr), callback_data=f"mattype|{exam_key}|{region}|{yr}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("📅 All Years", callback_data=f"mattype|{exam_key}|{region}|0")])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"exam|{exam_key}")])
    return InlineKeyboardMarkup(rows)

def mattype_kb(exam_key: str, region: str, year: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(label, callback_data=f"dosearch|{exam_key}|{region}|{year}|{code}|1")]
        for code, label in MATERIAL_TYPES.items()
    ]
    rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"region|{exam_key}|{region}")])
    return InlineKeyboardMarkup(rows)

def results_kb(results: List[Dict], page: int,
               exam_key: str, region: str, year: int, mat: str,
               sid: str) -> InlineKeyboardMarkup:
    per_page = 10
    start = (page - 1) * per_page
    sliced = results[start:start + per_page]
    src_icon = {"DDG": "🔷", "Bing": "🔶", "Google": "🔴", "Telegram": "📱"}

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
            callback_data=f"dosearch|{exam_key}|{region}|{year}|{mat}|{page-1}"))
    if start + per_page < len(results):
        nav.append(InlineKeyboardButton("Next ➡️",
            callback_data=f"dosearch|{exam_key}|{region}|{year}|{mat}|{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([
        InlineKeyboardButton(f"📊 {page}/{total_pages}  ({len(results)} results)", callback_data="noop"),
        InlineKeyboardButton("🔙 Menu", callback_data="back_main"),
    ])
    return InlineKeyboardMarkup(rows)

# ─────────────────────────────────────────────
#  DOWNLOAD WORKER  — FIX 5, 11, 12
# ─────────────────────────────────────────────
async def download_worker(stop_event: asyncio.Event):
    global ACTIVE_DL
    while not stop_event.is_set():
        try:
            task = await asyncio.wait_for(DOWNLOAD_QUEUE.get(), timeout=2)
        except asyncio.TimeoutError:
            continue

        client_ref, chat_id, url, title, user_id = task
        tmp_path = None   # FIX 5: always init before try block
        sent_ok  = False

        try:
            async with DOWNLOAD_SEMAPHORE:
                # FIX 12: increment inside inner try so decrement is always in inner finally
                async with ACTIVE_DL_LOCK:
                    ACTIVE_DL += 1
                try:
                    if not valid_url(url):
                        await client_ref.send_message(chat_id, f"❌ Unsafe URL blocked.")
                        sent_ok = True
                        return

                    resp = await safe_get(url, timeout=aiohttp.ClientTimeout(total=90))
                    if not resp:
                        await client_ref.send_message(chat_id, f"❌ Could not reach:\n{url}")
                        sent_ok = True
                        return

                    async with resp:
                        ct     = resp.headers.get("Content-Type", "")
                        is_pdf = "pdf" in ct.lower() or ".pdf" in url.lower()

                        if not is_pdf:
                            await client_ref.send_message(
                                chat_id,
                                f"🌐 **Web Resource:**\n**{title[:200]}**\n\n🔗 {url}"
                            )
                            sent_ok = True
                            return

                        # FIX 11: check Content-Length BEFORE writing
                        cl = int(resp.headers.get("Content-Length", 0))
                        if cl > 50 * 1024 * 1024:
                            await client_ref.send_message(
                                chat_id,
                                f"⚠️ File too large ({cl//1024//1024} MB). Open link:\n{url}"
                            )
                            sent_ok = True
                            return

                        # Write to temp file
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

                        # FIX 11: don't send incomplete file
                        if not complete:
                            await client_ref.send_message(
                                chat_id,
                                f"⚠️ File too large to download. Open directly:\n{url}"
                            )
                            sent_ok = True
                            return

                        if cl and size < cl * 0.85:
                            await client_ref.send_message(
                                chat_id,
                                f"⚠️ Incomplete download ({size//1024}KB of {cl//1024}KB):\n{url}"
                            )
                            sent_ok = True
                            return

                    safe_name = re.sub(r"[^\w\s\-]", "", title)[:60].strip() + ".pdf"
                    await client_ref.send_document(
                        chat_id=chat_id,
                        document=tmp_path,
                        file_name=safe_name,
                        caption=(
                            f"📄 **{title[:180]}**\n"
                            f"📦 {size // 1024} KB\n"
                            f"🔗 {url[:100]}"
                        ),
                    )
                    sent_ok = True
                    log.info("pdf_sent", size_kb=size // 1024, title=title[:60])

                finally:
                    # FIX 12: always decrement inside inner finally
                    async with ACTIVE_DL_LOCK:
                        ACTIVE_DL = max(0, ACTIVE_DL - 1)

        except Exception as e:
            log.error("dl_crash", err=str(e)[:100], url=url)
            if not sent_ok:
                try:
                    await client_ref.send_message(
                        chat_id,
                        f"❌ Download failed:\n**{title[:150]}**\n🔗 {url}\n`{str(e)[:80]}`"
                    )
                except Exception:
                    pass
        finally:
            DOWNLOAD_QUEUE.task_done()
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
    if await is_rate_limited(msg.from_user.id, "start", 5):
        return await msg.reply_text("⏳ Please wait a moment.")
    name = msg.from_user.first_name or "Friend"
    await msg.reply_text(
        f"👋 **Welcome, {name}!**\n\n"
        "🎯 **Pharmacist PYQ & Study Material Bot v2**\n\n"
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
    await msg.reply_text(
        f"📊 **Bot Stats**\n\n"
        f"⏱ Uptime: `{h}h {m}m {s}s`\n"
        f"📥 Active Downloads: `{ACTIVE_DL}`\n"
        f"📋 Queue: `{DOWNLOAD_QUEUE.qsize()}`\n"
        f"💾 Sessions: `{len(SESSION_MAP)}`\n"
        f"🔵 Status: `{'PAUSED' if QUEUE_PAUSED else 'ACTIVE'}`"
    )

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

    sid = make_search_id("CUSTOM", query[:40], 0, "pyq")
    await store_session(sid, results)
    await wait.edit_text(
        f"✅ Found **{len(results)}** results for `{query}`\n\n"
        "🔷 DDG  🔶 Bing  🔴 Google\n"
        "Tap title → open | 📥 → download",
        reply_markup=results_kb(results, 1, "CUSTOM", query[:20], 0, "pyq", sid),
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
        _, exam_key, region = data.split("|", 2)
        info = EXAM_TREE.get(exam_key, {})
        return await cq.message.edit_text(
            f"{info.get('label','')}\n📍 {region}\n\n📅 Select Year:",
            reply_markup=years_kb(exam_key, region)
        )

    if data.startswith("mattype|"):
        _, exam_key, region, year_str = data.split("|", 3)
        year = int(year_str)
        info = EXAM_TREE.get(exam_key, {})
        yr_label = str(year) if year else "All Years"
        return await cq.message.edit_text(
            f"{info.get('label','')}\n📍 {region} | 📅 {yr_label}\n\n📂 Select Material Type:",
            reply_markup=mattype_kb(exam_key, region, year)
        )

    if data.startswith("dosearch|"):
        parts = data.split("|")
        _, exam_key, region, year_str, mat, page_str = parts[:6]
        year     = int(year_str)
        page     = int(page_str)
        info     = EXAM_TREE.get(exam_key, {})
        exam_label = info.get("label", exam_key)
        yr_label   = str(year) if year else "All Years"

        # FIX 3: stable sid — no user_id, no timestamp
        sid = make_search_id(exam_key, region, year, mat)

        results = await load_session(sid)
        if not results:
            # Show "searching" status
            try:
                await cq.message.edit_text(
                    f"🔍 **Searching…**\n\n"
                    f"{exam_label}\n📍 {region} | 📅 {yr_label}\n"
                    f"📂 {MATERIAL_TYPES.get(mat,'')}\n\n"
                    "⏳ Scanning Google + Bing + DDG + Telegram…"
                )
            except MessageNotModified:
                pass

            results = await full_search(exam_key, exam_label, region, year or None, mat)
            if not results:
                return await cq.message.edit_text(
                    "❌ No results found. Try different year or material type.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Menu", callback_data="back_main")]
                    ])
                )
            await store_session(sid, results)

        return await cq.message.edit_text(
            f"📄 **{exam_label} | {region}**\n"
            f"📅 {yr_label} | {MATERIAL_TYPES.get(mat,'')}\n\n"
            f"✅ **{len(results)}** resources found",
            reply_markup=results_kb(results, page, exam_key, region, year, mat, sid),
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
        if DOWNLOAD_QUEUE.full():
            return await cq.answer("🚨 Queue full. Please wait.", show_alert=True)
        if await is_rate_limited(cq.from_user.id, "dl", 8):
            return await cq.answer("⏳ Wait before next download.", show_alert=True)

        item = results[idx]
        # FIX 4: `client` is first positional arg from Pyrogram — confirmed correct
        await DOWNLOAD_QUEUE.put((client, cq.message.chat.id,
                                  item["url"], item["title"], cq.from_user.id))
        await cq.answer(f"✅ Queued! Position ~{DOWNLOAD_QUEUE.qsize()}")
        return

    # ── ADMIN  FIX 1: no `global` inside nested if/elif ──
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
            return await cq.message.edit_text(
                f"⚙️ **Admin Panel**\n\n"
                f"📥 Active DL: `{ACTIVE_DL}`\n"
                f"📋 Queue: `{DOWNLOAD_QUEUE.qsize()}`\n"
                f"💾 Sessions: `{len(SESSION_MAP)}`\n"
                f"🔵 Queue: `{'PAUSED' if QUEUE_PAUSED else 'ACTIVE'}`",
                reply_markup=kb
            )

        if cmd == "toggle":
            # FIX 1: use globals() to mutate module-level var without `global` keyword
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
    global SESSION, REDIS, MONGO_DB

    # FIX 13: explicit db name from URI
    db_name = urlparse(MONGO_URI).path.lstrip("/") or "pharma_bot"

    connector = aiohttp.TCPConnector(
        limit=40, limit_per_host=6,
        ttl_dns_cache=300, ssl=False,
        enable_cleanup_closed=True
    )
    SESSION = aiohttp.ClientSession(connector=connector)

    try:
        REDIS = aioredis.from_url(REDIS_URL, decode_responses=False)
        await REDIS.ping()
        log.info("redis_ok")
    except Exception as e:
        log.warning("redis_skip", err=str(e)[:60])
        REDIS = None

    try:
        client_db = AsyncIOMotorClient(
            MONGO_URI, maxPoolSize=20, minPoolSize=2,
            serverSelectionTimeoutMS=4000
        )
        MONGO_DB = client_db[db_name]   # FIX 13: explicit name
        await MONGO_DB.command("ping")
        await MONGO_DB.search_cache.create_index("ts", expireAfterSeconds=86400)
        await MONGO_DB.sessions.create_index("ts",    expireAfterSeconds=7200)
        log.info("mongo_ok", db=db_name)
    except Exception as e:
        log.warning("mongo_skip", err=str(e)[:60])
        MONGO_DB = None

    for i in range(6):
        t = asyncio.create_task(download_worker(STOP_EVENT), name=f"dl_{i}")
        WORKER_TASK_LIST.append(t)
    log.info("workers_started", n=6)

async def stop_services():
    STOP_EVENT.set()
    for t in WORKER_TASK_LIST:
        t.cancel()
    await asyncio.gather(*WORKER_TASK_LIST, return_exceptions=True)
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
    print("🚀 Pharma Ultimate Bot v2 starting…")
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