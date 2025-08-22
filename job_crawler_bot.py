import os
import re
import csv
import json
import time
import asyncio
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime
from telegram import Bot
from dotenv import load_dotenv

# ----------- CONFIG -----------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()

CSV_PATH = "cleaned_file.csv"
SENT_STORE_PATH = "sent_jobs.json"
REQUEST_TIMEOUT = 15
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

# CSV log
LOG_FILE = "jobs_log.csv"
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Date", "Company", "Job Role", "Location", "Apply Link"])

# Enable JS render via .env or hardcode
ENV_JS = os.getenv("ENABLE_JS_RENDER", "false").strip().lower() in {"1", "true", "yes", "on"}
ENABLE_JS_RENDER = ENV_JS  # you can also set True/False directly

KEYWORDS = [
    "career", "careers", "job", "jobs", "opening", "openings",
    "vacancy", "vacancies", "apply", "opportunities", "recruitment"
]

DATE_OUTPUT_FMT = "%d/%m/%Y"  # 22/08/2025

# ----------- TELEGRAM -----------
if not BOT_TOKEN or not CHAT_ID:
    raise RuntimeError("BOT_TOKEN / CHAT_ID missing. Set them in .env")

bot = Bot(token=BOT_TOKEN)

# ----------- OPTIONAL: Selenium fallback (lazy init) -----------
driver = None
selenium_ready = False

def init_selenium_if_needed():
    """Init Selenium lazily only when required; auto-manage chromedriver."""
    global driver, selenium_ready, ENABLE_JS_RENDER
    if not ENABLE_JS_RENDER or selenium_ready:
        return

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from webdriver_manager.chrome import ChromeDriverManager

        chrome_opts = Options()
        chrome_opts.add_argument("--headless=new")
        chrome_opts.add_argument("--no-sandbox")
        chrome_opts.add_argument("--disable-dev-shm-usage")
        chrome_opts.add_argument(f"--user-agent={USER_AGENT}")
        chrome_opts.add_argument("--window-size=1366,768")

        driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_opts)
        selenium_ready = True
        print("[Selenium] Headless Chrome initialized.")
    except Exception as e:
        print(f"[WARN] Selenium init failed (JS render disabled): {e}")
        driver = None
        selenium_ready = False
        # Don’t flip ENABLE_JS_RENDER to False; keep user intent, but proceed without JS.

def js_get_html(url: str) -> str | None:
    """Render with Selenium (if available)."""
    init_selenium_if_needed()
    if not selenium_ready or driver is None:
        return None
    try:
        driver.get(url)
        time.sleep(2.5)  # allow render
        return driver.page_source
    except Exception as e:
        print(f"[ERROR] Selenium GET {url}: {e}")
        return None

# ----------- PERSIST ----------
def load_sent() -> set:
    if os.path.exists(SENT_STORE_PATH):
        try:
            with open(SENT_STORE_PATH, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()

def save_sent(s: set):
    try:
        with open(SENT_STORE_PATH, "w", encoding="utf-8") as f:
            json.dump(sorted(list(s)), f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] Couldn't save sent store: {e}")

sent_jobs = load_sent()

# ----------- HTTP -----------
def get_html(url: str) -> str | None:
    try:
        headers = {"User-Agent": USER_AGENT}
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.text
        print(f"[HTTP {resp.status_code}] {url}")
        return None
    except Exception as e:
        print(f"[ERROR] GET {url}: {e}")
        return None

def page_looks_unrendered(html: str) -> bool:
    """Heuristic: if page is too script-heavy / low text / no anchors with keywords, try JS render."""
    if not html:
        return True
    soup = BeautifulSoup(html, "html.parser")
    text_len = len(soup.get_text(" ").strip())
    anchors = soup.find_all("a", href=True)
    has_kw = any(looks_like_job_text(a.get_text()) or looks_like_job_url(a.get("href", "")) for a in anchors)
    many_scripts = len(soup.find_all("script")) > 20
    return (text_len < 800 and many_scripts) or (not has_kw and many_scripts)

def fetch_page(url: str) -> str | None:
    """Requests first; if looks unrendered and JS enabled, try Selenium."""
    html = get_html(url)
    if html:
        if ENABLE_JS_RENDER and page_looks_unrendered(html):
            rendered = js_get_html(url)
            return rendered or html
        return html
    # requests failed → try JS if enabled
    if ENABLE_JS_RENDER:
        return js_get_html(url)
    return None

# ----------- UTIL -----------
def normalize_url(base: str, href: str) -> str:
    absolute = urljoin(base, href)
    parsed = urlparse(absolute)
    return parsed._replace(fragment="").geturl()

def looks_like_job_text(text: str) -> bool:
    t = (text or "").strip().lower()
    return any(k in t for k in KEYWORDS)

def looks_like_job_url(url: str) -> bool:
    u = (url or "").lower()
    return any(k in u for k in KEYWORDS)

def collapse_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

# ----------- DATE PARSING -----------
DATE_PATTERNS = [
    r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",
    r"\b\d{4}[/-]\d{1,2}[/-]\d{1,2}\b",
    r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?\s+\d{1,2},?\s+\d{2,4}\b",
    r"\b\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?,?\s+\d{2,4}\b",
]
DATE_RE = re.compile("|".join(DATE_PATTERNS), re.IGNORECASE)

def parse_any_date(s: str) -> datetime | None:
    s = s.strip()
    fmts = [
        "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y",
        "%d/%m/%y", "%d-%m-%y", "%Y/%m/%d",
        "%b %d %Y", "%b %d, %Y", "%d %b %Y", "%d %b, %Y",
        "%B %d %Y", "%B %d, %Y", "%d %B %Y", "%d %B, %Y",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None

def extract_date_from_text(text: str) -> str | None:
    if not text:
        return None
    m = DATE_RE.search(text)
    if not m:
        return None
    raw = m.group(0)
    dt = parse_any_date(raw)
    if dt:
        return dt.strftime(DATE_OUTPUT_FMT)
    return raw

def extract_date_from_soup(soup: BeautifulSoup) -> str | None:
    # JSON-LD JobPosting
    for tag in soup.find_all("script", type=lambda v: v and "ld+json" in v):
        try:
            data = json.loads(tag.string or "{}")
            items = data if isinstance(data, list) else [data]
            for it in items:
                t = (it.get("@type") or it.get("type") or "")
                if isinstance(t, list):
                    is_job = any(x.lower() == "jobposting" for x in t if isinstance(x, str))
                else:
                    is_job = isinstance(t, str) and t.lower() == "jobposting"
                if is_job:
                    date_posted = it.get("datePosted") or it.get("dateposted")
                    if date_posted:
                        try:
                            dt = datetime.fromisoformat(date_posted.replace("Z", "+00:00"))
                            return dt.strftime(DATE_OUTPUT_FMT)
                        except Exception:
                            return extract_date_from_text(date_posted)
        except Exception:
            pass

    # <time> tags
    for tm in soup.find_all("time"):
        dt_attr = tm.get("datetime") or tm.get("aria-label") or ""
        txt = collapse_spaces(tm.get_text())
        val = extract_date_from_text(dt_attr or txt)
        if val:
            return val

    # meta tags
    for name in ["article:published_time", "og:updated_time", "pubdate", "publishdate", "date"]:
        m = soup.find("meta", attrs={"property": name}) or soup.find("meta", attrs={"name": name})
        if m and (m.get("content")):
            val = extract_date_from_text(m["content"])
            if val:
                return val

    # labels
    labels = ["posted", "date posted", "published", "updated", "last updated"]
    for lbl in labels:
        el = soup.find(string=re.compile(lbl, re.IGNORECASE))
        if el:
            around = collapse_spaces(el.parent.get_text()) if el.parent else ""
            val = extract_date_from_text(around)
            if val:
                return val

    body_text = collapse_spaces(soup.get_text(" "))
    return extract_date_from_text(body_text)

# ----------- TITLE -----------
def extract_title_from_soup(soup: BeautifulSoup, fallback: str = "N/A") -> str:
    if soup.title and soup.title.string:
        t = collapse_spaces(soup.title.string)
        if 5 <= len(t) <= 140:
            return t
    for tag in ["h1", "h2", "h3", "h4"]:
        h = soup.find(tag)
        if h:
            txt = collapse_spaces(h.get_text())
            if 3 <= len(txt) <= 140:
                return txt
    return fallback

# ----------- SANITIZE MD -----------
def md(text: str) -> str:
    return (text or "").replace("_", "\\_").replace("*", "\\*").replace("`", "\\`")

# ----------- TELEGRAM SEND -----------
async def send_job(company: str, title: str, posted_date: str | None, link: str, location: str = "Not specified"):
    company_md = md(company)
    title_md = md(title if title and title != "N/A" else "Job opening")
    date_txt = posted_date if posted_date else "Not specified"

    message = (
        f"💼 {title_md}\n"
        f"🏢 *{company_md}*\n"
        f"📅 Posted: {date_txt}\n"
        f"📍 Location: {location}\n"
        f"🔗 {link}"
    )
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="Markdown")

        with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([date_txt, company, title, location, link])

        sent_jobs.add(link)
        await asyncio.sleep(0.5)

    except Exception as e:
        print(f"[Telegram ERROR]: {e}")

# ----------- CORE CRAWL -----------
def extract_location_hint(soup: BeautifulSoup) -> str | None:
    """Lightweight location hint from page text (best effort)."""
    txt = soup.get_text(" ")
    # very simple heuristic; extend as needed
    m = re.search(r"\b(India|Bengaluru|Bangalore|Hyderabad|Chennai|Pune|Mumbai|Gurugram|Noida|Kolkata|Delhi)\b", txt, re.IGNORECASE)
    if m:
        return m.group(0).title()
    return None

async def crawl_jobs_once():
    print("🔍 Crawling started...")
    try:
        df = pd.read_csv(CSV_PATH)
    except Exception as e:
        print(f"[ERROR] Can't read {CSV_PATH}: {e}")
        return

    for col in ("Company Name", "Website"):
        if col not in df.columns:
            print(f"[ERROR] CSV missing required column: {col}")
            return

    new_count = 0
    for _, row in df.iterrows():
        company = str(row["Company Name"]).strip()
        site = str(row["Website"]).strip()
        if not site.startswith("http"):
            print(f"[SKIP] Invalid URL for {company}: {site}")
            continue

        html = fetch_page(site)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        anchors = soup.find_all("a", href=True)
        seen_here = set()

        for a in anchors:
            href = a.get("href", "").strip()
            text = collapse_spaces(a.get_text(" ").strip())
            if not href:
                continue

            full_link = normalize_url(site, href)
            if full_link in seen_here:
                continue
            seen_here.add(full_link)

            if not (looks_like_job_text(text) or looks_like_job_url(full_link)):
                continue

            if full_link in sent_jobs:
                continue

            # Visit job/careers page to extract accurate info
            job_html = fetch_page(full_link)
            if job_html:
                job_soup = BeautifulSoup(job_html, "html.parser")
                title = extract_title_from_soup(job_soup, fallback=text or "N/A")
                posted = extract_date_from_soup(job_soup)
                location_hint = extract_location_hint(job_soup) or "Not specified"
            else:
                title = text or "N/A"
                posted = None
                location_hint = "Not specified"

            await send_job(company, title, posted, full_link, location_hint)
            print(f"✅ Sent: {company} -> {full_link}")
            sent_jobs.add(full_link)
            new_count += 1

            save_sent(sent_jobs)  # persist after each send
            await asyncio.sleep(0.5)

    if new_count == 0:
        print("ℹ️ No new jobs found this run.")
    else:
        print(f"🎉 {new_count} new jobs sent.")

# ----------- MAIN LOOP (5 hours) -----------
async def main():
    print("✅ Bot is running (every 5 hours)…")
    while True:
        print(f"⏱️ Run at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        await crawl_jobs_once()
        print("😴 Sleeping for 5 hours…\n")
        await asyncio.sleep(5 * 60 * 60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("👋 Stopped. Saving…")
        save_sent(sent_jobs)
    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass
