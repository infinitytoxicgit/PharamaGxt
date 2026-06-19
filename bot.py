"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          PHARMA ULTIMATE BOT — Single File Enterprise Grade                 ║
║  AIIMS CRE | Drug Inspector | Govt Pharmacist | PYQ | Syllabus | Notes     ║
║  Multi-Engine: DDG + Google + Bing + Telegram Channels                     ║
╚══════════════════════════════════════════════════════════════════════════════╝
SETUP:
  pip install pyrogram tgcrypto motor redis aiohttp aiofiles structlog \
              beautifulsoup4 python-dotenv

.env file:
  API_ID=26950458
  API_HASH=d818b8d530e4a9b209509815ab1b9c7c
  BOT_TOKEN=
  MONGO_URI=mongodb://localhost:27017/pharma_bot
  REDIS_URL=redis://localhost:6379/0
  ADMIN_IDS=8676835917,8564072723
"""

# ─────────────────────────────────────────────
#  IMPORTS
# ─────────────────────────────────────────────
import os, sys, asyncio, aiohttp, aiofiles, json, zlib, hashlib
import time, uuid, random, re, tempfile, logging
from datetime import datetime
from urllib.parse import urlparse, quote_plus, unquote
from functools import wraps
from dataclasses import dataclass, field
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
#  ENVIRONMENT CONFIG
# ─────────────────────────────────────────────
API_ID       = int(os.getenv("API_ID", "0"))
API_HASH     = os.getenv("API_HASH", "")
BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
MONGO_URI    = os.getenv("MONGO_URI", "mongodb://localhost:27017/pharma_bot")
REDIS_URL    = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ADMIN_IDS    = [int(x) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.strip().isdigit()]

# ─────────────────────────────────────────────
#  CONSTANTS & EXAM TAXONOMY
# ─────────────────────────────────────────────
YEARS = list(range(2024, 2014, -1))   # 2024 → 2015

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
            "Drug Inspector Bihar", "Drug Inspector Rajasthan", "Drug Inspector Delhi",
            "Drug Inspector Maharashtra", "Drug Inspector Gujarat",
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

# Material categories
MATERIAL_TYPES = {
    "pyq":      "📝 Previous Year Papers",
    "syllabus": "📋 Syllabus",
    "notes":    "📖 Study Notes / Material",
    "anskey":   "✅ Answer Keys",
    "mock":     "🎯 Mock Tests",
    "books":    "📗 Reference Books PDF",
}

# Telegram public channels known to have pharmacy material
TELEGRAM_CHANNELS = [
    "pharmacist_pyq",
    "pharma_exam_material",
    "aiims_pharmacist_pyq",
    "drug_inspector_notes",
    "pharmacy_exam_pdf",
    "gpat_material",
    "pharmacist_exam_zone",
    "pharma_study_hub",
]

# Search engine templates
SEARCH_ENGINES = {
    "ddg":   "https://html.duckduckgo.com/html/?q={query}",
    "bing":  "https://www.bing.com/search?q={query}&count=50",
    "google_custom": "https://www.google.com/search?q={query}&num=30&as_filetype=pdf",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

# ─────────────────────────────────────────────
#  GLOBAL STATE
# ─────────────────────────────────────────────
SESSION: Optional[aiohttp.ClientSession] = None
REDIS: Optional[aioredis.Redis] = None
MONGO_DB = None
DOWNLOAD_QUEUE: asyncio.Queue = asyncio.Queue(maxsize=200)
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(6)
ACTIVE_DL = 0
ACTIVE_DL_LOCK = asyncio.Lock()
QUEUE_PAUSED = False
BOT_START_TIME = time.time()

# ─────────────────────────────────────────────
#  PYROGRAM CLIENT
# ─────────────────────────────────────────────
app = Client(
    "pharma_ultimate",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

# ─────────────────────────────────────────────
#  UTILITY: SAFE HANDLER WRAPPER
# ─────────────────────────────────────────────
def safe(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        tid = uuid.uuid4().hex[:8]
        try:
            return await func(*args, **kwargs)
        except FloodWait as fw:
            await asyncio.sleep(fw.value)
            return await func(*args, **kwargs)
        except MessageNotModified:
            pass
        except Exception as e:
            log.error("handler_crash", fn=func.__name__, err=str(e), tid=tid)
            for a in args:
                if isinstance(a, Message):
                    await a.reply_text(f"❌ Error occurred. Trace: `{tid}`")
                    break
                elif isinstance(a, CallbackQuery):
                    try:
                        await a.answer(f"⚠️ Error. Trace: {tid}", show_alert=True)
                    except Exception:
                        pass
                    break
    return wrapper

# ─────────────────────────────────────────────
#  UTILITY: REDIS HELPERS
# ─────────────────────────────────────────────
async def is_rate_limited(user_id: int, prefix: str, ttl: int = 5) -> bool:
    if not REDIS:
        return False
    key = f"rl:{prefix}:{user_id}"
    if await REDIS.get(key):
        return True
    await REDIS.setex(key, ttl, "1")
    return False

async def redis_get(key: str) -> Optional[bytes]:
    if not REDIS:
        return None
    try:
        return await REDIS.get(key)
    except Exception:
        return None

async def redis_set(key: str, value: bytes, ttl: int = 3600):
    if not REDIS:
        return
    try:
        await REDIS.setex(key, ttl, value)
    except Exception:
        pass

# ─────────────────────────────────────────────
#  UTILITY: SEARCH ID + COMPRESSION
# ─────────────────────────────────────────────
def make_search_id(user_id: int, query: str) -> str:
    raw = f"{user_id}|{query}|{time.strftime('%Y%m%d%H')}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]

def compress(data: list) -> bytes:
    return zlib.compress(json.dumps(data).encode(), level=6)

def decompress(b: bytes) -> list:
    return json.loads(zlib.decompress(b).decode())

# ─────────────────────────────────────────────
#  UTILITY: URL VALIDATION (SSRF SHIELD)
# ─────────────────────────────────────────────
def valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return False
        h = p.netloc.lower()
        for bad in ("localhost", "127.", "0.0.0.0", "169.254.", "::1"):
            if bad in h:
                return False
        if h.startswith(("10.", "192.168.", "172.16.", "172.17.", "172.18.",
                          "172.19.", "172.2", "172.3")):
            return False
        return True
    except Exception:
        return False

# ─────────────────────────────────────────────
#  BUILD SEARCH QUERIES from exam + year + material type
# ─────────────────────────────────────────────
def build_queries(exam_label: str, year: Optional[int], mat_type: str) -> List[str]:
    """Generate multiple search query strings for maximum coverage."""
    base = exam_label.replace("🏥 ", "").replace("🏨 ", "").replace("🏛️ ", "") \
                     .replace("🎓 ", "").replace("💊 ", "").replace("📚 ", "") \
                     .replace("🌿 ", "").replace("🗺️ ", "").replace("📖 ", "").strip()

    yr = str(year) if year else ""

    templates = {
        "pyq": [
            f"{base} pharmacist previous year question paper {yr} pdf",
            f"{base} pharmacist PYQ {yr} pdf download",
            f"{base} pharmacist solved paper {yr}",
            f"{base} pharmacist question paper {yr} memory based",
            f"{base} {yr} pharmacist paper pdf filetype:pdf",
        ],
        "syllabus": [
            f"{base} pharmacist syllabus {yr} pdf",
            f"{base} pharmacist exam pattern syllabus {yr}",
            f"{base} pharmacist detailed syllabus pdf download",
        ],
        "notes": [
            f"{base} pharmacist study material {yr} pdf",
            f"{base} pharmacist notes pdf download",
            f"pharmacist exam handwritten notes pdf",
            f"pharmacy exam important notes {yr} pdf",
            f"pharmacist competitive exam notes pdf free download",
        ],
        "anskey": [
            f"{base} pharmacist answer key {yr} pdf",
            f"{base} pharmacist official answer key {yr}",
            f"{base} answer key {yr} pdf download",
        ],
        "mock": [
            f"{base} pharmacist mock test {yr} pdf",
            f"pharmacist model question paper {yr} pdf",
            f"pharmacy competitive exam practice set pdf {yr}",
        ],
        "books": [
            f"pharmacist competitive exam book pdf free download",
            f"pharmacy D Pharma B Pharma notes pdf {yr}",
            f"RPS Malik pharmacist book pdf",
            f"pharmacist exam standard book pdf download",
        ],
    }

    queries = templates.get(mat_type, templates["pyq"])

    # Add Google-filetype variants
    google_q = [f"{q} filetype:pdf" for q in queries[:3]]
    return queries + google_q

# ─────────────────────────────────────────────
#  SCRAPER: DUCKDUCKGO
# ─────────────────────────────────────────────
async def scrape_ddg(query: str) -> List[Dict]:
    results = []
    try:
        url = SEARCH_ENGINES["ddg"].format(query=quote_plus(query + " filetype:pdf"))
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://duckduckgo.com/",
        }
        async with SESSION.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=25)) as r:
            html = await r.text()
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", class_="result__a"):
            href = a.get("href", "")
            if "uddg=" in href:
                href = unquote(href.split("uddg=")[1].split("&")[0])
            if href.startswith("http"):
                results.append({"title": a.get_text(strip=True)[:140], "url": href, "source": "DDG"})
    except Exception as e:
        log.warning("ddg_fail", err=str(e))
    return results

# ─────────────────────────────────────────────
#  SCRAPER: BING
# ─────────────────────────────────────────────
async def scrape_bing(query: str) -> List[Dict]:
    results = []
    try:
        url = SEARCH_ENGINES["bing"].format(query=quote_plus(query + " filetype:pdf"))
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9",
        }
        async with SESSION.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=25)) as r:
            html = await r.text()
        soup = BeautifulSoup(html, "html.parser")
        for li in soup.find_all("li", class_="b_algo"):
            a = li.find("a")
            if a:
                href = a.get("href", "")
                title = a.get_text(strip=True)
                if href.startswith("http"):
                    results.append({"title": title[:140], "url": href, "source": "Bing"})
    except Exception as e:
        log.warning("bing_fail", err=str(e))
    return results

# ─────────────────────────────────────────────
#  SCRAPER: GOOGLE (lite)
# ─────────────────────────────────────────────
async def scrape_google(query: str) -> List[Dict]:
    results = []
    try:
        url = SEARCH_ENGINES["google_custom"].format(query=quote_plus(query))
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
        }
        async with SESSION.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=25)) as r:
            html = await r.text()
        soup = BeautifulSoup(html, "html.parser")
        # Google result links
        for div in soup.find_all("div", class_=["yuRUbf", "tF2Cxc"]):
            a = div.find("a")
            if a:
                href = a.get("href", "")
                title_el = div.find("h3")
                title = title_el.get_text(strip=True) if title_el else href[:80]
                if href.startswith("http"):
                    results.append({"title": title[:140], "url": href, "source": "Google"})
        # fallback for result links
        if not results:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("/url?q="):
                    href = unquote(href[7:].split("&")[0])
                if href.startswith("http") and ".pdf" in href.lower():
                    results.append({"title": a.get_text(strip=True)[:140] or href[:80], "url": href, "source": "Google"})
    except Exception as e:
        log.warning("google_fail", err=str(e))
    return results

# ─────────────────────────────────────────────
#  SCRAPER: SPECIFIC EDU / GOV SITES
# ─────────────────────────────────────────────
DIRECT_SITES = [
    "https://aiimsexams.ac.in",
    "https://esic.nic.in",
    "https://dsssb.delhi.gov.in",
    "https://ruhsraj.org",
    "https://nhm.gov.in",
    "https://pharmacycouncil.nic.in",
    "https://cdsco.gov.in",
]

async def scrape_direct_site(site: str, keyword: str) -> List[Dict]:
    results = []
    try:
        url = f"{site}/search?q={quote_plus(keyword)}"
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        async with SESSION.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
            html = await r.text()
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.startswith("http"):
                href = site.rstrip("/") + "/" + href.lstrip("/")
            if ".pdf" in href.lower():
                results.append({
                    "title": a.get_text(strip=True)[:140] or href.split("/")[-1],
                    "url": href,
                    "source": site.replace("https://", "").split("/")[0]
                })
    except Exception:
        pass
    return results

# ─────────────────────────────────────────────
#  TELEGRAM PUBLIC CHANNEL SEARCH (via t.me search)
# ─────────────────────────────────────────────
async def search_telegram_channels(keyword: str) -> List[Dict]:
    """Search known public pharmacy Telegram channels via preview pages."""
    results = []
    for channel in TELEGRAM_CHANNELS:
        try:
            url = f"https://t.me/s/{channel}"
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            async with SESSION.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200:
                    continue
                html = await r.text()
            soup = BeautifulSoup(html, "html.parser")
            for msg in soup.find_all("div", class_="tgme_widget_message_wrap"):
                text_el = msg.find("div", class_="tgme_widget_message_text")
                link_el = msg.find("a", href=True)
                text = text_el.get_text(" ", strip=True)[:200] if text_el else ""
                if any(kw in text.lower() for kw in keyword.lower().split()):
                    link = link_el["href"] if link_el else f"https://t.me/{channel}"
                    results.append({
                        "title": f"[TG @{channel}] {text[:100]}",
                        "url": link,
                        "source": f"Telegram:{channel}"
                    })
                    if len(results) >= 5:
                        break
        except Exception:
            pass
        await asyncio.sleep(0.3)
    return results

# ─────────────────────────────────────────────
#  COMBINED SEARCH ENGINE (Cached)
# ─────────────────────────────────────────────
async def full_search(user_id: int, exam_label: str, year: Optional[int],
                      mat_type: str) -> List[Dict]:
    """Run all engines, merge, deduplicate, cache."""
    cache_key = f"res:{hashlib.md5(f'{exam_label}:{year}:{mat_type}'.encode()).hexdigest()}"

    # Try Redis cache first
    cached = await redis_get(cache_key)
    if cached:
        log.info("cache_hit", key=cache_key)
        return decompress(cached)

    # Check MongoDB cache
    if MONGO_DB is not None:
        doc = await MONGO_DB.search_cache.find_one({"_id": cache_key})
        if doc:
            results = decompress(doc["data"])
            await redis_set(cache_key, doc["data"], ttl=3600)
            return results

    queries = build_queries(exam_label, year, mat_type)
    log.info("search_start", queries_count=len(queries), exam=exam_label, year=year, mat=mat_type)

    # Run scraper tasks concurrently
    all_results = []
    scraper_tasks = []

    # Web engines for first 4 queries
    for q in queries[:5]:
        scraper_tasks.append(scrape_ddg(q))
        scraper_tasks.append(scrape_bing(q))
    for q in queries[:3]:
        scraper_tasks.append(scrape_google(q))

    # Direct gov sites
    base_kw = exam_label.split()[-1] + " pharmacist"
    for site in DIRECT_SITES[:3]:
        scraper_tasks.append(scrape_direct_site(site, base_kw))

    # Telegram
    tg_kw = f"{exam_label} pharmacist {year or ''} pdf"
    scraper_tasks.append(search_telegram_channels(tg_kw))

    batch_results = await asyncio.gather(*scraper_tasks, return_exceptions=True)
    for br in batch_results:
        if isinstance(br, list):
            all_results.extend(br)

    # Dedup + filter
    seen = set()
    filtered = []
    bad_words = {"admit card", "notification", "recruitment", "registration",
                 "vacancy", "apply online", "jobs", "admit", "hall ticket"}
    for item in all_results:
        url = item.get("url", "")
        if not url or url in seen:
            continue
        if not valid_url(url):
            continue
        title_low = item.get("title", "").lower()
        url_low = url.lower()
        # Must be PDF or title mentions pdf
        is_pdf = url_low.endswith(".pdf") or ".pdf" in url_low or "pdf" in title_low
        # Skip bad words only if not a direct site hit
        is_bad = any(bw in title_low for bw in bad_words)
        if is_bad and "pdf" not in url_low:
            continue
        seen.add(url)
        # Score: pdf link = higher, gov site = higher, tg = medium
        score = 0
        if ".pdf" in url_low:
            score += 10
        if any(d in url_low for d in ["gov.in", "nic.in", "ac.in", "edu"]):
            score += 5
        if "telegram" in item.get("source", "").lower():
            score += 3
        if str(year) in url_low if year else False:
            score += 4
        item["score"] = score
        filtered.append(item)

    # Sort by score desc
    filtered.sort(key=lambda x: x.get("score", 0), reverse=True)
    final = filtered[:500]

    log.info("search_done", total_raw=len(all_results), deduped=len(final))

    # Cache results
    if final:
        compressed = compress(final)
        await redis_set(cache_key, compressed, ttl=7200)
        if MONGO_DB is not None:
            await MONGO_DB.search_cache.update_one(
                {"_id": cache_key},
                {"$set": {"data": compressed, "ts": datetime.utcnow()}},
                upsert=True
            )

    return final

# ─────────────────────────────────────────────
#  KEYBOARD BUILDERS
# ─────────────────────────────────────────────
def main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    buttons = []
    for key, info in EXAM_TREE.items():
        buttons.append([InlineKeyboardButton(info["label"], callback_data=f"exam|{key}")])
    buttons.append([
        InlineKeyboardButton("🔍 Custom Search", callback_data="custom_search"),
        InlineKeyboardButton("ℹ️ Help", callback_data="help"),
    ])
    if user_id in ADMIN_IDS:
        buttons.append([InlineKeyboardButton("⚙️ Admin Panel", callback_data="admin|dash")])
    return InlineKeyboardMarkup(buttons)

def regions_keyboard(exam_key: str) -> InlineKeyboardMarkup:
    info = EXAM_TREE[exam_key]
    buttons = []
    for region in info["regions"]:
        safe_r = region.replace("|", "-")
        buttons.append([InlineKeyboardButton(f"📍 {region}", callback_data=f"region|{exam_key}|{safe_r}")])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="back_main")])
    return InlineKeyboardMarkup(buttons)

def years_keyboard(exam_key: str, region: str) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    for yr in YEARS:
        safe_r = region.replace("|", "-")
        row.append(InlineKeyboardButton(str(yr), callback_data=f"mattype|{exam_key}|{safe_r}|{yr}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("📅 All Years", callback_data=f"mattype|{exam_key}|{region}|0")])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"exam|{exam_key}")])
    return InlineKeyboardMarkup(buttons)

def mattype_keyboard(exam_key: str, region: str, year: int) -> InlineKeyboardMarkup:
    buttons = []
    for code, label in MATERIAL_TYPES.items():
        buttons.append([InlineKeyboardButton(label, callback_data=f"search|{exam_key}|{region}|{year}|{code}|1")])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"region|{exam_key}|{region}")])
    return InlineKeyboardMarkup(buttons)

def results_keyboard(results: List[Dict], page: int,
                     exam_key: str, region: str, year: int, mat: str,
                     search_id: str) -> InlineKeyboardMarkup:
    per_page = 10
    start = (page - 1) * per_page
    end = start + per_page
    sliced = results[start:end]

    buttons = []
    for i, item in enumerate(sliced):
        g_idx = start + i
        src_badge = {"DDG": "🔷", "Bing": "🔶", "Google": "🔴"}.get(
            item.get("source", ""), "🔗")
        title = item["title"][:38] + "…" if len(item["title"]) > 38 else item["title"]
        btn_title = f"{src_badge}[{g_idx+1}] {title}"

        # PDF direct link vs webpage
        is_pdf = ".pdf" in item["url"].lower()
        dl_label = "📥 PDF" if is_pdf else "🌐 Open"

        buttons.append([
            InlineKeyboardButton(btn_title, url=item["url"]),
            InlineKeyboardButton(dl_label, callback_data=f"dl|{search_id}|{g_idx}"),
        ])

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"search|{exam_key}|{region}|{year}|{mat}|{page-1}"))
    if end < len(results):
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"search|{exam_key}|{region}|{year}|{mat}|{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([
        InlineKeyboardButton(f"📊 Page {page} / {max(1,(len(results)-1)//per_page+1)}", callback_data="noop"),
        InlineKeyboardButton("🔙 Menu", callback_data="back_main"),
    ])
    return InlineKeyboardMarkup(buttons)

# ─────────────────────────────────────────────
#  SESSION STORE (in-memory + Mongo fallback)
# ─────────────────────────────────────────────
SESSION_MAP: Dict[str, Dict] = {}   # search_id → {results, exam, region, year, mat}

async def store_session(search_id: str, data: Dict):
    SESSION_MAP[search_id] = data
    if MONGO_DB is not None:
        await MONGO_DB.sessions.update_one(
            {"_id": search_id},
            {"$set": {**data, "ts": datetime.utcnow()}},
            upsert=True
        )

async def get_session(search_id: str) -> Optional[Dict]:
    if search_id in SESSION_MAP:
        return SESSION_MAP[search_id]
    if MONGO_DB is not None:
        doc = await MONGO_DB.sessions.find_one({"_id": search_id})
        if doc:
            SESSION_MAP[search_id] = doc
            return doc
    return None

# ─────────────────────────────────────────────
#  DOWNLOAD WORKER
# ─────────────────────────────────────────────
async def download_worker(stop_event: asyncio.Event):
    global ACTIVE_DL
    while not stop_event.is_set():
        try:
            task = await asyncio.wait_for(DOWNLOAD_QUEUE.get(), timeout=2)
        except asyncio.TimeoutError:
            continue

        client_ref, chat_id, url, title, user_id = task
        tmp_path = None
        try:
            async with DOWNLOAD_SEMAPHORE:
                async with ACTIVE_DL_LOCK:
                    ACTIVE_DL += 1

                if not valid_url(url):
                    await client_ref.send_message(chat_id, "❌ Invalid or unsafe URL blocked.")
                    continue

                headers = {"User-Agent": random.choice(USER_AGENTS)}
                async with SESSION.get(url, headers=headers,
                                       timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    content_type = resp.headers.get("Content-Type", "")
                    is_pdf_ct = "pdf" in content_type.lower()
                    is_pdf_url = url.lower().endswith(".pdf") or ".pdf" in url.lower()

                    if not (is_pdf_ct or is_pdf_url):
                        # Send as link if not PDF
                        await client_ref.send_message(
                            chat_id,
                            f"🌐 **Resource (Web Page):**\n{title}\n\n🔗 {url}"
                        )
                        continue

                    # Download to temp file
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
                    tmp_path = tmp.name
                    tmp.close()

                    size = 0
                    async with aiofiles.open(tmp_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(4096):
                            await f.write(chunk)
                            size += len(chunk)
                            if size > 50 * 1024 * 1024:  # 50 MB limit
                                break

                safe_name = re.sub(r"[^\w\s\-]", "", title)[:60].strip() + ".pdf"
                await client_ref.send_document(
                    chat_id=chat_id,
                    document=tmp_path,
                    file_name=safe_name,
                    caption=(
                        f"📄 **{title[:200]}**\n"
                        f"🔗 Source: {url[:100]}\n"
                        f"📦 Size: {size//1024} KB"
                    ),
                )
                log.info("pdf_sent", title=title, size=size)

        except Exception as e:
            log.error("dl_worker_err", err=str(e), url=url)
            try:
                await client_ref.send_message(
                    chat_id,
                    f"❌ Download failed for:\n**{title}**\n🔗 {url}\n\nError: `{str(e)[:100]}`"
                )
            except Exception:
                pass
        finally:
            DOWNLOAD_QUEUE.task_done()
            async with ACTIVE_DL_LOCK:
                ACTIVE_DL = max(0, ACTIVE_DL - 1)
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

# ─────────────────────────────────────────────
#  HANDLERS
# ─────────────────────────────────────────────

@app.on_message(filters.command("start") & filters.private)
@safe
async def cmd_start(_, msg: Message):
    if await is_rate_limited(msg.from_user.id, "start", 5):
        return await msg.reply_text("⏳ Please wait a moment before restarting.")
    name = msg.from_user.first_name or "Friend"
    text = (
        f"👋 **Welcome, {name}!**\n\n"
        "🎯 **Pharma PYQ & Study Material Bot**\n\n"
        "✅ AIIMS CRE | ESIC | DSSSB | NHM\n"
        "✅ Drug Inspector | GPAT | State PSC\n"
        "✅ Previous Year Papers | Syllabus | Notes\n"
        "✅ Answer Keys | Mock Tests | Books\n\n"
        "🔍 Multi-engine search: Google + Bing + DDG + Telegram Channels\n\n"
        "👇 **Select your exam below:**"
    )
    await msg.reply_text(text, reply_markup=main_menu_keyboard(msg.from_user.id))


@app.on_message(filters.command("stats") & filters.private)
@safe
async def cmd_stats(_, msg: Message):
    uptime = int(time.time() - BOT_START_TIME)
    h, r = divmod(uptime, 3600)
    m, s = divmod(r, 60)
    q_size = DOWNLOAD_QUEUE.qsize()
    text = (
        f"📊 **Bot Statistics**\n\n"
        f"⏱️ Uptime: `{h}h {m}m {s}s`\n"
        f"📥 Active Downloads: `{ACTIVE_DL}`\n"
        f"📋 Queue Size: `{q_size}`\n"
        f"🔵 Status: {'✅ Active' if not QUEUE_PAUSED else '⏸️ Paused'}\n"
    )
    await msg.reply_text(text)


@app.on_message(filters.command("search") & filters.private)
@safe
async def cmd_search(_, msg: Message):
    """Direct text search: /search AIIMS pharmacist 2023"""
    query = msg.text.replace("/search", "").strip()
    if not query:
        return await msg.reply_text("Usage: `/search AIIMS pharmacist 2023 pdf`")
    wait = await msg.reply_text(f"🔍 Searching: `{query}`...")
    results = []
    tasks = [scrape_ddg(query), scrape_bing(query), scrape_google(query)]
    batch = await asyncio.gather(*tasks, return_exceptions=True)
    for b in batch:
        if isinstance(b, list):
            results.extend(b)
    seen = set()
    deduped = []
    for r in results:
        if r["url"] not in seen and valid_url(r["url"]):
            seen.add(r["url"])
            deduped.append(r)
    deduped = deduped[:200]

    if not deduped:
        return await wait.edit_text("❌ No results found. Try different keywords.")

    sid = make_search_id(msg.from_user.id, query)
    await store_session(sid, {
        "results": compress(deduped).hex(),
        "exam": "CUSTOM",
        "region": query[:30],
        "year": 0,
        "mat": "pyq",
    })
    kb = results_keyboard(deduped, 1, "CUSTOM", query[:20], 0, "pyq", sid)
    await wait.edit_text(
        f"✅ Found **{len(deduped)}** results for `{query}`\n\n"
        "🔷 DDG  🔶 Bing  🔴 Google\n"
        "Click **title** to open | **📥 PDF** to download",
        reply_markup=kb
    )


# ─── CALLBACK ROUTER ───────────────────────────────────────────
@app.on_callback_query()
@safe
async def callback_router(client, cq: CallbackQuery):
    data = cq.data

    # ── noop ──
    if data == "noop":
        return await cq.answer()

    # ── help ──
    if data == "help":
        text = (
            "📖 **How to Use**\n\n"
            "1️⃣ Select your exam from the menu\n"
            "2️⃣ Choose region/institute\n"
            "3️⃣ Select year (or All Years)\n"
            "4️⃣ Choose material type\n"
            "5️⃣ Browse results & tap 📥 to download\n\n"
            "🔍 **Direct Search:** /search AIIMS pharmacist 2023\n\n"
            "📌 Sources: Google + Bing + DDG + Gov Sites + Telegram Channels\n"
            "⚡ Results are cached for faster re-access"
        )
        await cq.message.edit_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Main Menu", callback_data="back_main")]
        ]))
        return

    # ── back to main ──
    if data == "back_main":
        await cq.message.edit_text(
            "👇 **Select your exam:**",
            reply_markup=main_menu_keyboard(cq.from_user.id)
        )
        return

    # ── exam selected ──
    if data.startswith("exam|"):
        exam_key = data.split("|")[1]
        info = EXAM_TREE.get(exam_key)
        if not info:
            return await cq.answer("Unknown exam", show_alert=True)
        await cq.message.edit_text(
            f"{info['label']}\n\n📍 Select Region / Institute:",
            reply_markup=regions_keyboard(exam_key)
        )
        return

    # ── region selected ──
    if data.startswith("region|"):
        _, exam_key, region = data.split("|", 2)
        info = EXAM_TREE.get(exam_key, {})
        await cq.message.edit_text(
            f"{info.get('label','')}\n📍 {region}\n\n📅 Select Year:",
            reply_markup=years_keyboard(exam_key, region)
        )
        return

    # ── material type selection ──
    if data.startswith("mattype|"):
        parts = data.split("|")
        _, exam_key, region, year_str = parts[0], parts[1], parts[2], parts[3]
        year = int(year_str)
        info = EXAM_TREE.get(exam_key, {})
        yr_label = str(year) if year else "All Years"
        await cq.message.edit_text(
            f"{info.get('label','')}\n📍 {region} | 📅 {yr_label}\n\n📂 Select Material Type:",
            reply_markup=mattype_keyboard(exam_key, region, year)
        )
        return

    # ── SEARCH / PAGINATION ──
    if data.startswith("search|"):
        parts = data.split("|")
        _, exam_key, region, year_str, mat, page_str = parts
        year = int(year_str)
        page = int(page_str)
        info = EXAM_TREE.get(exam_key, {})
        exam_label = info.get("label", exam_key)

        sid = make_search_id(cq.from_user.id, f"{exam_key}{region}{year}{mat}")

        # Check session cache
        session = await get_session(sid)
        if session and "results" in session:
            results = decompress(bytes.fromhex(session["results"]))
        else:
            yr_label = str(year) if year else "All Years"
            await cq.message.edit_text(
                f"🔍 **Searching...**\n\n"
                f"🏷️ {exam_label}\n📍 {region}\n📅 {yr_label}\n📂 {MATERIAL_TYPES.get(mat,'')}\n\n"
                "⏳ Scanning Google + Bing + DDG + Gov Sites + Telegram..."
            )
            results = await full_search(cq.from_user.id, f"{exam_label} {region}", year or None, mat)
            if not results:
                await cq.message.edit_text(
                    "❌ No results found for this combination.\n\nTry:\n• Different year\n• Different material type\n• /search command with custom keywords",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu", callback_data="back_main")]])
                )
                return
            await store_session(sid, {
                "results": compress(results).hex(),
                "exam": exam_key, "region": region,
                "year": year, "mat": mat,
            })

        yr_label = str(year) if year else "All Years"
        per_page = 10
        total_pages = max(1, (len(results) - 1) // per_page + 1)

        await cq.message.edit_text(
            f"📄 **{exam_label} | {region}**\n"
            f"📅 {yr_label} | {MATERIAL_TYPES.get(mat,'')}\n\n"
            f"✅ Found **{len(results)}** resources | Page {page}/{total_pages}\n\n"
            "🔷 DDG  🔶 Bing  🔴 Google  🔗 Direct\n"
            "Tap title → open  |  📥 → download PDF",
            reply_markup=results_keyboard(results, page, exam_key, region, year, mat, sid),
            disable_web_page_preview=True
        )
        return

    # ── DOWNLOAD TRIGGER ──
    if data.startswith("dl|"):
        if QUEUE_PAUSED:
            return await cq.answer("⏸️ Downloads paused. Try later.", show_alert=True)

        _, sid, idx_str = data.split("|", 2)
        idx = int(idx_str)

        session = await get_session(sid)
        if not session:
            return await cq.answer("❌ Session expired. Please search again.", show_alert=True)

        results = decompress(bytes.fromhex(session["results"]))
        if idx >= len(results):
            return await cq.answer("❌ Item not found.", show_alert=True)

        if DOWNLOAD_QUEUE.full():
            return await cq.answer("🚨 Queue full. Please wait and try again.", show_alert=True)

        item = results[idx]
        url = item["url"]
        title = item["title"]

        # Rate limit: max 5 DL requests per 30 sec per user
        if await is_rate_limited(cq.from_user.id, "dl", 6):
            return await cq.answer("⏳ Please wait before requesting another download.", show_alert=True)

        await DOWNLOAD_QUEUE.put((client, cq.message.chat.id, url, title, cq.from_user.id))
        await cq.answer(f"✅ Queued! Position: ~{DOWNLOAD_QUEUE.qsize()}", show_alert=False)
        return

    # ── ADMIN PANEL ──
    if data.startswith("admin|"):
        if cq.from_user.id not in ADMIN_IDS:
            return await cq.answer("❌ Unauthorized", show_alert=True)
        cmd = data.split("|")[1]

        if cmd == "dash":
            text = (
                f"⚙️ **Admin Control Panel**\n\n"
                f"📥 Active Downloads: `{ACTIVE_DL}`\n"
                f"📋 Queue Size: `{DOWNLOAD_QUEUE.qsize()}`\n"
                f"⏱️ Uptime: `{int(time.time()-BOT_START_TIME)}s`\n"
                f"🔵 Queue: `{'PAUSED' if QUEUE_PAUSED else 'ACTIVE'}`\n"
                f"💾 Session Cache: `{len(SESSION_MAP)}` entries"
            )
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "⏸️ Pause Queue" if not QUEUE_PAUSED else "▶️ Resume Queue",
                    callback_data="admin|toggle_queue"
                )],
                [InlineKeyboardButton("🗑️ Clear Session Cache", callback_data="admin|clear_cache")],
                [InlineKeyboardButton("🔙 Main Menu", callback_data="back_main")],
            ])
            await cq.message.edit_text(text, reply_markup=kb)

        elif cmd == "toggle_queue":
            global QUEUE_PAUSED
            QUEUE_PAUSED = not QUEUE_PAUSED
            state = "PAUSED ⏸️" if QUEUE_PAUSED else "ACTIVE ▶️"
            await cq.answer(f"Queue is now {state}", show_alert=True)
            await cq.message.edit_text(
                f"Queue is now **{state}**",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin", callback_data="admin|dash")]])
            )

        elif cmd == "clear_cache":
            SESSION_MAP.clear()
            await cq.answer("✅ Session cache cleared.", show_alert=True)
        return

    # ── CUSTOM SEARCH PROMPT ──
    if data == "custom_search":
        await cq.message.edit_text(
            "🔍 **Custom Search**\n\n"
            "Send a message with:\n"
            "`/search <your keywords>`\n\n"
            "Examples:\n"
            "• `/search AIIMS pharmacist 2022 pdf`\n"
            "• `/search drug inspector MP 2021 question paper`\n"
            "• `/search GPAT previous year solved paper`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_main")]])
        )
        return

    await cq.answer("Unknown action", show_alert=True)

# ─────────────────────────────────────────────
#  BOT LIFECYCLE
# ─────────────────────────────────────────────
STOP_EVENT = asyncio.Event()
WORKER_TASKS = []

async def start_services():
    global SESSION, REDIS, MONGO_DB

    # HTTP Session
    connector = aiohttp.TCPConnector(limit=30, ttl_dns_cache=300, ssl=False)
    SESSION = aiohttp.ClientSession(connector=connector)

    # Redis
    try:
        REDIS = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=False)
        await REDIS.ping()
        log.info("redis_connected")
    except Exception as e:
        log.warning("redis_unavailable", err=str(e))
        REDIS = None

    # MongoDB
    try:
        db_client = AsyncIOMotorClient(MONGO_URI, maxPoolSize=30, minPoolSize=3, serverSelectionTimeoutMS=5000)
        MONGO_DB = db_client.get_default_database()
        await MONGO_DB.command("ping")
        # Indexes
        await MONGO_DB.search_cache.create_index("ts", expireAfterSeconds=86400)
        await MONGO_DB.sessions.create_index("ts", expireAfterSeconds=3600)
        log.info("mongo_connected")
    except Exception as e:
        log.warning("mongo_unavailable", err=str(e))
        MONGO_DB = None

    # Start 6 download workers
    for i in range(6):
        t = asyncio.create_task(download_worker(STOP_EVENT))
        WORKER_TASKS.append(t)
    log.info("workers_started", count=6)

async def stop_services():
    STOP_EVENT.set()
    for t in WORKER_TASKS:
        t.cancel()
    if SESSION and not SESSION.closed:
        await SESSION.close()
    if REDIS:
        await REDIS.aclose()
    log.info("services_stopped")

# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────
async def main():
    if not all([API_ID, API_HASH, BOT_TOKEN]):
        print("❌ Missing API_ID, API_HASH, or BOT_TOKEN in .env")
        sys.exit(1)

    await start_services()
    print("🚀 Pharma Ultimate Bot starting...")

    try:
        await app.start()
        me = await app.get_me()
        print(f"✅ Bot online: @{me.username}")

        # Keep running
        await asyncio.Event().wait()

    except KeyboardInterrupt:
        print("🛑 Shutdown requested...")
    finally:
        await stop_services()
        await app.stop()
        print("👋 Bot stopped cleanly.")

if __name__ == "__main__":
    asyncio.run(main())