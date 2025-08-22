import os, re, csv, json, time, asyncio, requests, pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
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
KEYWORDS = ["career","careers","job","jobs","opening","openings","vacancy","vacancies","apply","opportunities","recruitment"]
RESTRICTED_DOMAINS = ["facebook.com","linkedin.com","twitter.com","instagram.com"]
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
def normalize_url(base, href):
    absolute = urljoin(base, href)
    parsed = urlparse(absolute)
    return parsed._replace(fragment="").geturl()

def collapse_spaces(s: str):
    return re.sub(r"\s+"," ", s or "").strip()

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
        try: return datetime.strptime(s, fmt)
        except: continue
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
    for tm in soup.find_all("time"):
        dt_attr = tm.get("datetime") or tm.get("aria-label") or ""
        txt = collapse_spaces(tm.get_text())
        val = extract_date_from_text(dt_attr or txt)
        if val: return val
    return extract_date_from_text(collapse_spaces(soup.get_text()))

# ---------------- TITLE & LOCATION ----------------
def extract_title_from_soup(soup, fallback="N/A"):
    if soup.title and soup.title.string:
        t = collapse_spaces(soup.title.string)
        if 5<=len(t)<=140: return t
    for tag in ["h1","h2","h3","h4"]:
        h = soup.find(tag)
        if h:
            txt = collapse_spaces(h.get_text())
            if 3<=len(txt)<=140: return txt
    return fallback

def extract_location_from_soup(soup):
    txt = soup.get_text(" ")
    m = re.search(r"\b(India|Bengaluru|Bangalore|Hyderabad|Chennai|Pune|Mumbai|Gurugram|Noida|Kolkata|Delhi)\b", txt, re.IGNORECASE)
    if m: return m.group(0).title()
    return "Not specified"

# ---------------- TELEGRAM ----------------
def md(text): return (text or "").replace("_","\\_").replace("*","\\*").replace("`","\\`")

async def send_job(company,title,posted_date,link,location):
    message = (f"💼 {md(title or 'Job opening')}\n🏢 *{md(company)}*\n📅 Posted: {posted_date or 'Not specified'}\n📍 Location: {location}\n🔗 {link}")
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="Markdown")
        with open(LOG_FILE,"a",newline="",encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([posted_date or "",company,title,location,link])
        sent_jobs.add(link)
        await asyncio.sleep(0.5)
    except: pass

# ---------------- CRAWL ----------------
def is_recent(posted_date):
    if not posted_date: return False
    try:
        dt = datetime.strptime(posted_date, DATE_OUTPUT_FMT)
        return datetime.now()-dt <= timedelta(days=2)  # <-- 2-day filter
    except: return False

async def crawl_jobs_once():
    print("🔍 Crawling started...")
    try: df = pd.read_csv(CSV_PATH)
    except:
        print(f"[ERROR] Can't read {CSV_PATH}"); return

    for col in ("Company Name","Website"):
        if col not in df.columns:
            print(f"[ERROR] CSV missing: {col}"); return

    new_count=0
    for _,row in df.iterrows():
        company=str(row["Company Name"]).strip()
        site=str(row["Website"]).strip()
        if not site.startswith("http"): continue

        html=fetch_page(site)
        if not html: continue
        soup=BeautifulSoup(html,"html.parser")
        anchors=soup.find_all("a",href=True)
        seen_here=set()

        for a in anchors:
            href=a.get("href").strip()
            if not href or href.lower().startswith("javascript:") or href.startswith("#"): continue
            full_link=normalize_url(site,href)
            if full_link in seen_here or full_link in sent_jobs: continue
            seen_here.add(full_link)

            job_html=fetch_page(full_link)
            if not job_html: continue
            job_soup=BeautifulSoup(job_html,"html.parser")

            title=extract_title_from_soup(job_soup, fallback=a.get_text(" ").strip())
            posted=extract_date_from_soup(job_soup)
            if not is_recent(posted): continue
            location=extract_location_from_soup(job_soup)

            await send_job(company,title,posted,full_link,location)
            print(f"✅ Sent: {company} -> {full_link}")
            save_sent(sent_jobs)
            new_count+=1

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

# Run bot in background thread
def run_bot():
    import asyncio
    asyncio.run(main())

threading.Thread(target=run_bot, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

print("🚀 Bot ready for Render Web Service.")
