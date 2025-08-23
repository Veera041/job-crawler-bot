import os, re, csv, json, time, asyncio, requests, pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit, parse_qsl, urlencode
from datetime import datetime
from telegram import Bot
from dotenv import load_dotenv

# ---------------- CONFIG ----------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()
CSV_PATH = "cleaned_file.csv"
SENT_STORE_PATH = "sent_jobs.json"
REQUEST_TIMEOUT = 15
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0 Safari/537.36")
LOG_FILE = "jobs_log.csv"

if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Date","Company","Job Role","Location","Apply Link"])

ENABLE_JS_RENDER = os.getenv("ENABLE_JS_RENDER","false").strip().lower() in {"1","true","yes","on"}

CAREER_KEYWORDS = [
    "career", "careers", "job", "jobs", "openings", "opportunities",
    "vacancy", "vacancies", "recruit", "join-us", "joinus", "work-with-us",
    "workwithus", "talent", "positions", "hiring"
]
EXCLUDE_PATTERNS = [
    "blog", "event", "events", "news", "press", "media", "article", "insight",
    "stories", "webinar", "podcast", "case-study", "casestudy", "whitepaper"
]
RESTRICTED_DOMAINS = ["facebook.com","linkedin.com","twitter.com","instagram.com","x.com","youtube.com"]

DATE_OUTPUT_FMT = "%d/%m/%Y"

if not BOT_TOKEN or not CHAT_ID:
    raise RuntimeError("BOT_TOKEN / CHAT_ID missing in .env")
bot = Bot(token=BOT_TOKEN)

# ---------------- SELENIUM INIT ----------------
driver = None
selenium_ready = False
def init_selenium_if_needed():
    global driver, selenium_ready
    if not ENABLE_JS_RENDER or selenium_ready:
        return
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager

        chrome_opts = Options()
        chrome_opts.add_argument("--headless=new")
        chrome_opts.add_argument("--no-sandbox")
        chrome_opts.add_argument("--disable-dev-shm-usage")
        chrome_opts.add_argument(f"--user-agent={USER_AGENT}")
        chrome_opts.add_argument("--window-size=1366,768")
        chrome_opts.add_argument("--log-level=3")
        chrome_opts.add_experimental_option("excludeSwitches", ["enable-logging"])
        chrome_opts.add_experimental_option('useAutomationExtension', False)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_opts)
        selenium_ready = True
        print("[Selenium] Headless Chrome initialized.")
    except Exception as e:
        print(f"[WARN] Selenium init failed: {e}")
        driver = None
        selenium_ready = False

def js_get_html(url: str):
    init_selenium_if_needed()
    if not selenium_ready or driver is None:
        return None
    try:
        driver.get(url)
        time.sleep(2)
        return driver.page_source
    except:
        return None

# ---------------- PERSISTENCE ----------------
def load_sent():
    if os.path.exists(SENT_STORE_PATH):
        try:
            with open(SENT_STORE_PATH,"r",encoding="utf-8") as f:
                return set(json.load(f))
        except:
            return set()
    return set()

def save_sent(s):
    try:
        with open(SENT_STORE_PATH,"w",encoding="utf-8") as f:
            json.dump(sorted(list(s)), f, ensure_ascii=False, indent=2)
    except:
        pass

sent_jobs = load_sent()

# ---------------- HTTP FETCH ----------------
def get_html(url: str):
    if not url.startswith("http"):
        return None
    for d in RESTRICTED_DOMAINS:
        if d in url.lower():
            return None
    try:
        headers = {"User-Agent": USER_AGENT}
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.text
        return None
    except:
        return None

def fetch_page(url: str):
    html = get_html(url)
    if html:
        return html
    if ENABLE_JS_RENDER:
        return js_get_html(url)
    return None

# ---------------- UTILS ----------------
def collapse_spaces(s: str):
    return re.sub(r"\s+"," ", s or "").strip()

def canonicalize_url(url: str):
    try:
        parts = urlsplit(url)
        fragment = ""
        q = []
        for k,v in parse_qsl(parts.query, keep_blank_values=True):
            lk = k.lower()
            if lk.startswith("utm_") or lk in {"gclid","fbclid","mc_cid","mc_eid","igshid"}:
                continue
            q.append((k,v))
        query = urlencode(q, doseq=True)
        netloc = parts.netloc.lower()
        cleaned = urlunsplit((parts.scheme, netloc, parts.path, query, fragment))
        if cleaned.endswith("//"):
            cleaned = cleaned[:-1]
        return cleaned
    except:
        return url

def normalize_url(base, href):
    absolute = urljoin(base, href)
    absolute = canonicalize_url(absolute)
    parsed = urlparse(absolute)
    return parsed._replace(fragment="").geturl()

def same_host(a: str, b: str):
    try:
        return urlparse(a).netloc.split(":")[0].lower() == urlparse(b).netloc.split(":")[0].lower()
    except:
        return False

# ---------------- DATE PARSING ----------------
DATE_PATTERNS = [
    r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",
    r"\b\d{4}[/-]\d{1,2}[/-]\d{1,2}\b",
    r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{1,2},?\s+\d{2,4}\b",
    r"\b\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?,?\s+\d{2,4}\b"
]
DATE_RE = re.compile("|".join(DATE_PATTERNS), re.IGNORECASE)

def parse_any_date(s):
    s = s.strip()
    fmts = ["%d/%m/%Y","%d-%m-%Y","%Y-%m-%d","%m/%d/%Y","%m-%d-%Y",
            "%d/%m/%y","%d-%m-%y","%Y/%m/%d",
            "%b %d %Y","%b %d, %Y","%d %b %Y","%d %b, %Y",
            "%B %d %Y","%B %d, %Y","%d %B %Y","%d %B, %Y"]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt)
        except:
            continue
    return None

def extract_date_from_text(text):
    if not text: return None
    m = DATE_RE.search(text)
    if not m: return None
    raw = m.group(0)
    dt = parse_any_date(raw)
    if dt: return dt.strftime(DATE_OUTPUT_FMT)
    return raw

def extract_date_from_soup(soup):
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            items = data if isinstance(data, list) else [data]
            for it in items:
                if isinstance(it, dict) and str(it.get("@type","")).lower() == "jobposting":
                    dp = it.get("datePosted") or it.get("validThrough")
                    if dp:
                        dt = parse_any_date(dp) or parse_any_date(dp.replace("T"," ").split("+")[0])
                        if dt: return dt.strftime(DATE_OUTPUT_FMT)
        except:
            pass
    for tm in soup.find_all("time"):
        dt_attr = tm.get("datetime") or tm.get("aria-label") or ""
        txt = collapse_spaces(tm.get_text())
        val = extract_date_from_text(dt_attr or txt)
        if val: return val
    return extract_date_from_text(collapse_spaces(soup.get_text()))

# ---------------- TITLE & LOCATION ----------------
def extract_title_from_soup(soup, fallback="N/A"):
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        t = collapse_spaces(og.get("content"))
        if 3 <= len(t) <= 140: return t
    if soup.title and soup.title.string:
        t = collapse_spaces(soup.title.string)
        t = re.sub(r"\s+[-|–].*$", "", t).strip()
        if 3 <= len(t) <= 140: return t
    for tag in ["h1","h2","h3","h4"]:
        h = soup.find(tag)
        if h:
            txt = collapse_spaces(h.get_text())
            if 3 <= len(txt) <= 140: return txt
    return fallback

CITY_WORDS = r"(?:India|Bengaluru|Bangalore|Hyderabad|Chennai|Pune|Mumbai|Gurugram|Gurgaon|Noida|Kolkata|Delhi|Remote|Anywhere)"
def extract_location_from_soup(soup):
    try:
        for script in soup.find_all("script", type="application/ld+json"):
            data = json.loads(script.string or "")
            items = data if isinstance(data, list) else [data]
            for it in items:
                if isinstance(it, dict) and str(it.get("@type","")).lower() == "jobposting":
                    jl = it.get("jobLocation")
                    if isinstance(jl, dict):
                        addr = jl.get("address", {})
                        parts = [addr.get("addressLocality"), addr.get("addressRegion"), addr.get("addressCountry")]
                        loc = ", ".join([p for p in parts if p])
                        if loc: return loc
    except:
        pass
    txt = soup.get_text(" ")
    m = re.search(rf"\b{CITY_WORDS}\b", txt, re.IGNORECASE)
    if m: return m.group(0).title()
    return "Not specified"

# ---------------- JOB PAGE HEURISTICS ----------------
def looks_like_job_posting(soup, url: str):
    u = url.lower()
    if any(x in u for x in EXCLUDE_PATTERNS):
        return False
    try:
        for script in soup.find_all("script", type="application/ld+json"):
            data = json.loads(script.string or "")
            items = data if isinstance(data, list) else [data]
            for it in items:
                if isinstance(it, dict) and str(it.get("@type","")).lower() == "jobposting":
                    return True
    except:
        pass
    body = soup.get_text(" ").lower()
    signals = [
        "apply now", "apply", "responsibilities", "requirements", "job description",
        "what you will do", "role & responsibilities", "position summary"
    ]
    if sum(1 for s in signals if s in body) >= 2:
        return True
    if any(k in u for k in ["job","jobs","career","careers","opening","opportunity","position","vacancy"]):
        return True
    return False

# ---------------- TELEGRAM ----------------
async def send_job(company,title,posted_date,link,location):
    message = (
        f'role : "{collapse_spaces(title or "Job opening")}" , '
        f'company name : {collapse_spaces(company)} , '
        f'location : {collapse_spaces(location)} , '
        f'apply link : {link}'
    )
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode=None)
        with open(LOG_FILE,"a",newline="",encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([posted_date or "", company, title, location, link])
        sent_jobs.add(link)
        await asyncio.sleep(0.3)
    except Exception as e:
        print(f"[WARN] Telegram send failed: {e}")

# ---------------- DISCOVERY ----------------
def discover_career_pages(site_url: str, soup: BeautifulSoup):
    career_pages = set()
    for a in soup.find_all("a", href=True):
        txt = collapse_spaces(a.get_text()).lower()
        href = a["href"].strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        full = normalize_url(site_url, href)
        if not same_host(site_url, full):
            continue
        u = full.lower()
        if any(k in u for k in CAREER_KEYWORDS):
            career_pages.add(full)
    return career_pages or {site_url}

def extract_job_links(career_url: str, soup: BeautifulSoup):
    job_links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        full = normalize_url(career_url, href)
        u = full.lower()
        if any(x in u for x in EXCLUDE_PATTERNS):
            continue
        if any(k in u for k in ["job","jobs","opening","position","opportunity","careers","vacancy","gh_jid","lever.co","greenhouse.io","workable.com","smartrecruiters.com","myworkdayjobs.com"]):
            job_links.add(full)
    return job_links

# ---------------- CRAWL ----------------
async def crawl_jobs_once():
    print("🔍 Crawling started...")
    try:
        df = pd.read_csv(CSV_PATH)
    except Exception as e:
        print(f"[ERROR] Can't read {CSV_PATH}: {e}")
        return

    for col in ("Company Name","Website"):
        if col not in df.columns:
            print(f"[ERROR] CSV missing: {col}")
            return

    new_count = 0
    seen_global = set()

    for _, row in df.iterrows():
        company = str(row["Company Name"]).strip()
        site = str(row["Website"]).strip()
        if not site.startswith("http"):
            continue

        home_html = fetch_page(site)
        if not home_html:
            continue
        home_soup = BeautifulSoup(home_html, "html.parser")

        career_pages = discover_career_pages(site, home_soup)

        candidate_job_links = set()
        for page in career_pages:
            ch = fetch_page(page)
            if not ch:
                continue
            cs = BeautifulSoup(ch, "html.parser")
            candidate_job_links |= extract_job_links(page, cs)

        for link in candidate_job_links:
            link = canonicalize_url(link)
            if link in seen_global or link in sent_jobs:
                continue
            job_html = fetch_page(link)
            if not job_html:
                continue
            job_soup = BeautifulSoup(job_html, "html.parser")

            if not looks_like_job_posting(job_soup, link):
                continue

            title = extract_title_from_soup(job_soup, fallback="Job Opening")
            posted = extract_date_from_soup(job_soup)
            location = extract_location_from_soup(job_soup)

            await send_job(company, title, posted, link, location)
            print(f"✅ Sent: {company} -> {link}")
            save_sent(sent_jobs)
            seen_global.add(link)
            new_count += 1

    print(f"🎉 {new_count} jobs sent." if new_count else "ℹ️ No new jobs this run.")

# ---------------- MAIN LOOP ----------------
async def main():
    print("✅ Bot running (every 5 hours)...")
    while True:
        print(f"⏱️ Run at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        await crawl_jobs_once()
        print("😴 Sleeping 5 hours...\n")
        await asyncio.sleep(5*60*60)

# ---------------- FLASK WEB SERVER ----------------
from flask import Flask
import threading

app = Flask(__name__)

@app.route("/")
def home():
    return "✅ Job Crawler Bot is running!"

def run_bot():
    import asyncio
    asyncio.run(main())

threading.Thread(target=run_bot, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

print("🚀 Bot ready for Render Web Service.")
