"""
Buddhist Events Scraper
Runs weekly via GitHub Actions.
Scrapes major Buddhist centre websites and adds new events to Supabase.
"""

import os
import json
import re
import time
import hashlib
from datetime import datetime, date
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from html.parser import HTMLParser
import urllib.parse

# ── Config ────────────────────────────────────────────────────────────────────
SUPA_URL = os.environ.get("SUPABASE_URL", "")
SUPA_KEY = os.environ.get("SUPABASE_KEY", "")
TODAY    = date.today().isoformat()
ADDED    = 0
ERRORS   = []

# ── HTTP helper ───────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BuddhistEventsBot/1.0; +https://github.com/Global-Buddhist-Calendar)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

def fetch(url, timeout=15):
    try:
        req = Request(url, headers=HEADERS)
        with urlopen(req, timeout=timeout) as r:
            charset = "utf-8"
            ct = r.headers.get("Content-Type", "")
            if "charset=" in ct:
                charset = ct.split("charset=")[-1].strip().split(";")[0].strip()
            return r.read().decode(charset, errors="replace")
    except Exception as e:
        ERRORS.append(f"fetch({url}): {e}")
        return ""

# ── Supabase helpers ───────────────────────────────────────────────────────────
def supa_get(path, params=""):
    url = f"{SUPA_URL}/rest/v1/{path}{params}"
    req = Request(url, headers={
        "apikey": SUPA_KEY,
        "Authorization": f"Bearer {SUPA_KEY}",
        "Accept": "application/json",
    })
    try:
        with urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except Exception as e:
        ERRORS.append(f"supa_get({path}): {e}")
        return []

def supa_insert(event):
    url = f"{SUPA_URL}/rest/v1/events"
    data = json.dumps(event).encode("utf-8")
    req = Request(url, data=data, headers={
        "apikey": SUPA_KEY,
        "Authorization": f"Bearer {SUPA_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }, method="POST")
    try:
        with urlopen(req, timeout=15) as r:
            return r.status in (200, 201)
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        if "duplicate" in body.lower() or "unique" in body.lower():
            return False  # already exists, not an error
        ERRORS.append(f"supa_insert({event.get('title','?')}): {e} — {body[:200]}")
        return False
    except Exception as e:
        ERRORS.append(f"supa_insert({event.get('title','?')}): {e}")
        return False

def existing_titles():
    rows = supa_get("events", "?select=title&limit=5000")
    return {r["title"].lower().strip() for r in rows}

def make_event(title, date_str, end_date, location, continent, school, etype,
               description, teacher, organization, source_url,
               confidence="likely", confidence_note="Scraped automatically"):
    return {
        "title":           title.strip(),
        "date":            date_str,
        "end_date":        end_date,
        "location":        location.strip(),
        "continent":       continent,
        "school":          school,
        "type":            etype,
        "description":     description.strip(),
        "teacher":         teacher,
        "organization":    organization,
        "source_url":      source_url,
        "confidence":      confidence,
        "confidence_note": confidence_note,
        "approved":        True,
    }

def try_add(event, known):
    global ADDED
    key = event["title"].lower().strip()
    if key in known:
        return
    if supa_insert(event):
        known.add(key)
        ADDED += 1
        print(f"  ✓ Added: {event['title']}")
    else:
        print(f"  – Skipped (exists or error): {event['title']}")

# ── Date parsing helpers ───────────────────────────────────────────────────────
MONTH_MAP = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
    "january":1,"february":2,"march":3,"april":4,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
}

def parse_date_str(s):
    """Try to parse a date string into YYYY-MM-DD. Returns None on failure."""
    s = s.strip()
    # ISO format
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # DD Month YYYY or Month DD, YYYY
    m = re.match(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", s)
    if m:
        mo = MONTH_MAP.get(m.group(2).lower()[:3])
        if mo:
            return f"{m.group(3)}-{mo:02d}-{int(m.group(1)):02d}"
    m = re.match(r"([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})", s)
    if m:
        mo = MONTH_MAP.get(m.group(1).lower()[:3])
        if mo:
            return f"{m.group(3)}-{mo:02d}-{int(m.group(2)):02d}"
    return None

def future_date(d):
    return d is not None and d >= TODAY

# ── Continent detection ────────────────────────────────────────────────────────
CONTINENT_MAP = {
    "North America": ["usa","united states","canada","mexico","california","new york",
                      "massachusetts","washington","colorado","arizona","new mexico",
                      "oregon","texas","florida","virginia","illinois","michigan"],
    "Europe": ["uk","united kingdom","england","scotland","wales","ireland","france",
               "germany","italy","spain","portugal","netherlands","belgium","switzerland",
               "austria","sweden","norway","denmark","finland","poland","czech","hungary",
               "romania","greece","malta","devon","northumberland","london","paris","berlin"],
    "Asia": ["india","china","japan","korea","thailand","sri lanka","nepal","bhutan",
             "tibet","vietnam","cambodia","myanmar","indonesia","taiwan","singapore",
             "dharamsala","chiang rai","bangkok","tokyo","seoul","colombo"],
    "Online": ["online","virtual","zoom","livestream","webinar"],
}

def detect_continent(location):
    loc = location.lower()
    if any(k in loc for k in CONTINENT_MAP["Online"]):
        return "Online"
    for continent, keywords in CONTINENT_MAP.items():
        if any(k in loc for k in keywords):
            return continent
    return "Other"

# ══════════════════════════════════════════════════════════════════════════════
#  SCRAPERS
# ══════════════════════════════════════════════════════════════════════════════

# ── 1. Tushita Meditation Centre ──────────────────────────────────────────────
def scrape_tushita(known):
    print("\n── Tushita Meditation Centre ──")
    html = fetch("https://tushita.info/program/calendar/")
    if not html:
        return
    # Find event blocks with title and date
    blocks = re.findall(
        r'<article[^>]*>(.*?)</article>',
        html, re.DOTALL | re.IGNORECASE
    )
    for block in blocks:
        title_m = re.search(r'<h\d[^>]*>\s*<a[^>]*>([^<]+)</a>', block, re.IGNORECASE)
        date_m  = re.search(r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})', block)
        if not title_m or not date_m:
            continue
        title = re.sub(r'\s+', ' ', title_m.group(1)).strip()
        d = parse_date_str(date_m.group(1))
        if not d or not future_date(d):
            continue
        url_m = re.search(r'href="(https?://tushita\.info[^"]+)"', block, re.IGNORECASE)
        source_url = url_m.group(1) if url_m else "https://tushita.info/program/calendar/"
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Dharamsala, India", continent="Asia",
            school="Vajrayana", etype="Teachings",
            description=f"Event at Tushita Meditation Centre, Dharamsala.",
            teacher=None, organization="Tushita Meditation Centre",
            source_url=source_url,
        )
        try_add(ev, known)

# ── 2. Plum Village ───────────────────────────────────────────────────────────
def scrape_plum_village(known):
    print("\n── Plum Village ──")
    html = fetch("https://plumvillage.org/retreats/retreats-calendar")
    if not html:
        return
    # Find retreat cards
    titles = re.findall(r'<h\d[^>]*class="[^"]*entry-title[^"]*"[^>]*>\s*<a[^>]*>([^<]+)</a>', html, re.IGNORECASE)
    dates  = re.findall(r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})', html)
    urls   = re.findall(r'href="(https://plumvillage\.org/event/[^"]+)"', html, re.IGNORECASE)
    for i, title in enumerate(titles[:20]):
        title = title.strip()
        d = parse_date_str(dates[i]) if i < len(dates) else None
        if not d or not future_date(d):
            continue
        source_url = urls[i] if i < len(urls) else "https://plumvillage.org/retreats/retreats-calendar"
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Plum Village, France", continent="Europe",
            school="Zen", etype="Retreat",
            description="Retreat at Plum Village in the mindfulness tradition of Thich Nhat Hanh.",
            teacher="Plum Village Monastics", organization="Plum Village",
            source_url=source_url,
        )
        try_add(ev, known)

# ── 3. Gaia House ─────────────────────────────────────────────────────────────
def scrape_gaia_house(known):
    print("\n── Gaia House ──")
    html = fetch("https://gaiahouse.co.uk/programme/")
    if not html:
        return
    # Extract retreat links and titles
    items = re.findall(
        r'href="(https://gaiahouse\.co\.uk/(?:programme|gh)/[^"]+)"[^>]*>\s*([^<]{5,100})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items:
        title = re.sub(r'\s+', ' ', title).strip()
        if len(title) < 5 or title in seen:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Devon, UK", continent="Europe",
            school="Theravada", etype="Retreat",
            description="Insight Meditation retreat at Gaia House, Devon.",
            teacher=None, organization="Gaia House",
            source_url=url,
        )
        try_add(ev, known)

# ── 4. Spirit Rock ────────────────────────────────────────────────────────────
def scrape_spirit_rock(known):
    print("\n── Spirit Rock ──")
    html = fetch("https://www.spiritrock.org/programs/retreats")
    if not html:
        return
    titles = re.findall(r'<h\d[^>]*class="[^"]*program-title[^"]*"[^>]*>([^<]+)</h', html, re.IGNORECASE)
    dates  = re.findall(r'(\w+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|\w+ \d{1,2},?\s+\d{4})', html)
    urls   = re.findall(r'href="(/programs/retreats/[^"]+)"', html, re.IGNORECASE)
    for i, title in enumerate(titles[:15]):
        title = title.strip()
        d = parse_date_str(dates[i]) if i < len(dates) else None
        if not d or not future_date(d):
            continue
        source_url = "https://www.spiritrock.org" + urls[i] if i < len(urls) else "https://www.spiritrock.org/programs/retreats"
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Woodacre, California, USA", continent="North America",
            school="Theravada", etype="Retreat",
            description="Insight meditation retreat at Spirit Rock Meditation Center.",
            teacher=None, organization="Spirit Rock Meditation Center",
            source_url=source_url,
        )
        try_add(ev, known)

# ── 5. Insight Meditation Society (IMS) ───────────────────────────────────────
def scrape_ims(known):
    print("\n── Insight Meditation Society ──")
    html = fetch("https://www.dharma.org/retreats/schedules/retreat-center-schedule-2026/")
    if not html:
        html = fetch("https://www.dharma.org/retreats/schedules/")
    if not html:
        return
    # Look for retreat entries with dates
    blocks = re.findall(r'<li[^>]*>(.*?)</li>', html, re.DOTALL | re.IGNORECASE)
    for block in blocks:
        title_m = re.search(r'<a[^>]*>([^<]{10,120})</a>', block, re.IGNORECASE)
        date_m  = re.search(r'([A-Za-z]+ \d{1,2}[^<]{0,30}\d{4})', block)
        if not title_m or not date_m:
            continue
        title = re.sub(r'\s+', ' ', title_m.group(1)).strip()
        d = parse_date_str(date_m.group(1))
        if not d or not future_date(d):
            continue
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Barre, Massachusetts, USA", continent="North America",
            school="Theravada", etype="Retreat",
            description="Silent insight meditation retreat at the Insight Meditation Society.",
            teacher=None, organization="Insight Meditation Society",
            source_url="https://www.dharma.org/retreats/schedules/",
        )
        try_add(ev, known)

# ── 6. Throssel Hole Buddhist Abbey ──────────────────────────────────────────
def scrape_throssel(known):
    print("\n── Throssel Hole Buddhist Abbey ──")
    html = fetch("https://throssel.org.uk/calendar/")
    if not html:
        return
    # Find event entries from calendar
    events = re.findall(
        r'<a[^>]*href="(https://throssel\.org\.uk/event/[^"]+)"[^>]*>\s*([^<]{5,100})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'(\w+ \d{1,2},?\s+@?\s*\d{4}|\d{1,2}\s+\w+\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in events:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Northumberland, UK", continent="Europe",
            school="Zen", etype="Retreat",
            description="Retreat at Throssel Hole Buddhist Abbey in the Serene Reflection (Soto Zen) tradition.",
            teacher="Throssel Hole Monastics", organization="Throssel Hole Buddhist Abbey",
            source_url=url,
        )
        try_add(ev, known)

# ── 7. Upaya Zen Center ───────────────────────────────────────────────────────
def scrape_upaya(known):
    print("\n── Upaya Zen Center ──")
    html = fetch("https://www.upaya.org/programs/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://www.upaya.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Santa Fe, New Mexico, USA", continent="North America",
            school="Zen", etype="Retreat",
            description="Programme at Upaya Zen Center, Santa Fe.",
            teacher=None, organization="Upaya Zen Center",
            source_url=url,
        )
        try_add(ev, known)

# ── 8. Cloud Mountain Retreat Center ─────────────────────────────────────────
def scrape_cloud_mountain(known):
    print("\n── Cloud Mountain ──")
    html = fetch("https://cloudmountain.org/all-retreats/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:20]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://cloudmountain.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Castle Rock, Washington, USA", continent="North America",
            school="Theravada", etype="Retreat",
            description="Insight meditation retreat at Cloud Mountain Retreat Center.",
            teacher=None, organization="Cloud Mountain Retreat Center",
            source_url=url,
        )
        try_add(ev, known)

# ── 9. Garchen Buddhist Institute ────────────────────────────────────────────
def scrape_garchen(known):
    print("\n── Garchen Buddhist Institute ──")
    html = fetch("https://garchen.net/annual-events/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*(?:<a[^>]*>)?([^<]{5,120})(?:</a>)?</h',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for title in items[:15]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Chino Valley, Arizona, USA", continent="North America",
            school="Vajrayana", etype="Teachings",
            description="Teaching or retreat at Garchen Buddhist Institute.",
            teacher="H.E. Garchen Rinpoche", organization="Garchen Buddhist Institute",
            source_url="https://garchen.net/annual-events/",
        )
        try_add(ev, known)

# ── 10. Blue Cliff Monastery ──────────────────────────────────────────────────
def scrape_blue_cliff(known):
    print("\n── Blue Cliff Monastery ──")
    html = fetch("https://www.bluecliffmonastery.org/schedule-of-events")
    if not html:
        html = fetch("https://www.bluecliffmonastery.org/retreats-2026")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*(?:<a[^>]*href="([^"]*)"[^>]*>)?([^<]{5,120})(?:</a>)?</h',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:15]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        source = url if url and url.startswith("http") else "https://www.bluecliffmonastery.org/schedule-of-events"
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Pine Bush, New York, USA", continent="North America",
            school="Zen", etype="Retreat",
            description="Retreat at Blue Cliff Monastery in the Plum Village tradition.",
            teacher="Blue Cliff Monastics", organization="Blue Cliff Monastery",
            source_url=source,
        )
        try_add(ev, known)

# ── 11. Abhayagiri Buddhist Monastery ────────────────────────────────────────
def scrape_abhayagiri(known):
    print("\n── Abhayagiri Buddhist Monastery ──")
    html = fetch("https://www.abhayagiri.org/events")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:12]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://www.abhayagiri.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Redwood Valley, California, USA", continent="North America",
            school="Theravada", etype="Teachings",
            description="Teaching or retreat at Abhayagiri Buddhist Monastery in the Thai Forest Tradition.",
            teacher=None, organization="Abhayagiri Buddhist Monastery",
            source_url=url,
        )
        try_add(ev, known)

# ── 12. Drala Mountain Center ─────────────────────────────────────────────────
def scrape_drala(known):
    print("\n── Drala Mountain Center ──")
    html = fetch("https://www.dralamountain.org/programs/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*class="[^"]*program[^"]*"[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:12]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://www.dralamountain.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Red Feather Lakes, Colorado, USA", continent="North America",
            school="Vajrayana", etype="Retreat",
            description="Programme at Drala Mountain Center in the Shambhala Buddhist tradition.",
            teacher=None, organization="Drala Mountain Center",
            source_url=url,
        )
        try_add(ev, known)

# ── 13. Zen Mountain Monastery ────────────────────────────────────────────────
def scrape_zmm(known):
    print("\n── Zen Mountain Monastery ──")
    html = fetch("https://zmm.org/all-programs/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:15]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://zmm.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Mount Tremper, New York, USA", continent="North America",
            school="Zen", etype="Retreat",
            description="Programme at Zen Mountain Monastery in the Mountains and Rivers Order.",
            teacher=None, organization="Zen Mountain Monastery",
            source_url=url,
        )
        try_add(ev, known)

# ── 14. Insight Retreat Center (IRC) ─────────────────────────────────────────
def scrape_irc(known):
    print("\n── Insight Retreat Center ──")
    html = fetch("https://www.insightretreatcenter.org/retreats/")
    if not html:
        return
    # IRC uses a specific format
    blocks = re.findall(r'<strong>(.*?)</strong>(.*?)(?=<strong>|</div>|<hr)', html, re.DOTALL | re.IGNORECASE)
    for title_raw, rest in blocks[:20]:
        title = re.sub(r'<[^>]+>', '', title_raw).strip()
        title = re.sub(r'\s+', ' ', title)
        if len(title) < 5:
            continue
        date_m = re.search(r'([A-Za-z]+ \d{1,2} to \d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', rest)
        if not date_m:
            continue
        d = parse_date_str(date_m.group(1))
        if not d or not future_date(d):
            continue
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Santa Cruz, California, USA", continent="North America",
            school="Theravada", etype="Meditation",
            description="Silent insight meditation retreat at Insight Retreat Center, Santa Cruz. Free to attend.",
            teacher=None, organization="Insight Retreat Center",
            source_url="https://www.insightretreatcenter.org/retreats/",
        )
        try_add(ev, known)

# ── 15. Mountain Cloud Zen Center ────────────────────────────────────────────
def scrape_mountain_cloud(known):
    print("\n── Mountain Cloud Zen Center ──")
    html = fetch("https://www.mountaincloud.org/series/sesshin/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:10]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://www.mountaincloud.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Santa Fe, New Mexico, USA", continent="North America",
            school="Zen", etype="Retreat",
            description="Sesshin or retreat at Mountain Cloud Zen Center in the Sanbo Zen tradition.",
            teacher=None, organization="Mountain Cloud Zen Center",
            source_url=url,
        )
        try_add(ev, known)

# ── 16. Tibethaus Deutschland ─────────────────────────────────────────────────
def scrape_tibethaus(known):
    print("\n── Tibethaus Deutschland ──")
    html = fetch("https://www.tibethaus.com/en/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*(?:<a[^>]*href="([^"]*)"[^>]*>)?([^<]{5,120})(?:</a>)?</h',
        html, re.IGNORECASE
    )
    dates = re.findall(r'(\d{1,2}\.\d{1,2}\.\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:15]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        # Handle German date format DD.MM.YYYY
        raw_d = dates[date_idx] if date_idx < len(dates) else None
        date_idx += 1
        d = None
        if raw_d:
            m = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', raw_d)
            if m:
                d = f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
            else:
                d = parse_date_str(raw_d)
        if not d or not future_date(d):
            continue
        source = url if url and url.startswith("http") else "https://www.tibethaus.com/en/"
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Frankfurt, Germany", continent="Europe",
            school="Vajrayana", etype="Teachings",
            description="Teaching or event at Tibethaus Deutschland, Frankfurt.",
            teacher=None, organization="Tibethaus Deutschland",
            source_url=source,
        )
        try_add(ev, known)

# ── 17. Dhagpo Kundreul Ling ──────────────────────────────────────────────────
def scrape_dhagpo(known):
    print("\n── Dhagpo Kundreul Ling ──")
    html = fetch("https://dhagpo-kundreul-ling.org/en/programme-october-2025-may-2026/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>([^<]{5,120})</h',
        html, re.IGNORECASE
    )
    dates = re.findall(r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for title in items[:15]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Lacapelle-Livron, France", continent="Europe",
            school="Vajrayana", etype="Teachings",
            description="Teaching or retreat at Dhagpo Kundreul Ling in the Kagyu Tibetan Buddhist tradition.",
            teacher="Jigme Rinpoche", organization="Dhagpo Kundreul Ling",
            source_url="https://dhagpo-kundreul-ling.org/en/programme-october-2025-may-2026/",
        )
        try_add(ev, known)

# ── 18. BSWA / Jhana Grove ────────────────────────────────────────────────────
def scrape_bswa(known):
    print("\n── BSWA / Jhana Grove ──")
    html = fetch("https://bswa.org/events/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*class="[^"]*entry-title[^"]*"[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:12]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Serpentine, Western Australia", continent="Other",
            school="Theravada", etype="Meditation",
            description="Retreat or event at Jhana Grove with the Buddhist Society of Western Australia.",
            teacher=None, organization="Buddhist Society of Western Australia",
            source_url=url,
        )
        try_add(ev, known)

# ══════════════════════════════════════════════════════════════════════════════
#  ADDITIONAL SCRAPERS
# ══════════════════════════════════════════════════════════════════════════════

# ── 19. FPMT Global Retreat Schedule ─────────────────────────────────────────
def scrape_fpmt(known):
    print("\n── FPMT Global Retreats ──")
    html = fetch("https://fpmt.org/centers/retreat/schedule/")
    if not html:
        return
    # FPMT lists retreats in a structured format: date, title, centre
    blocks = re.findall(
        r'(\w+ \d{1,2}[-–]\d{1,2},?\s+\d{4}|\w+ \d{1,2},?\s+\d{4})\s*([^\n<]{10,120}?)(?:with|led by)?\s*(?:[^\n<]{0,60}?)([A-Za-z\s,]+(?:Centre|Center|Institute|Monastery)[^\n<]{0,60})',
        html, re.IGNORECASE
    )
    # Simpler fallback: find all date+title patterns
    entries = re.findall(
        r'(\w+\s+\d{1,2}[-–]\d{1,2},?\s+\d{4})\s+([^\n<]{10,100})',
        html, re.IGNORECASE
    )
    seen = set()
    for date_raw, title in entries[:30]:
        title = re.sub(r'\s+', ' ', title).strip().rstrip('.,')
        if title in seen or len(title) < 8:
            continue
        seen.add(title)
        d = parse_date_str(date_raw)
        if not d or not future_date(d):
            continue
        # Try to detect location from title
        loc = "Various locations"
        cont = "Other"
        for keyword, location, continent in [
            ("Australia", "Australia", "Other"),
            ("France", "France", "Europe"),
            ("Spain", "Spain", "Europe"),
            ("Italy", "Italy", "Europe"),
            ("UK", "United Kingdom", "Europe"),
            ("India", "India", "Asia"),
            ("USA", "USA", "North America"),
            ("Canada", "Canada", "North America"),
        ]:
            if keyword.lower() in title.lower():
                loc = location
                cont = continent
                break
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location=loc, continent=cont,
            school="Vajrayana", etype="Retreat",
            description="Retreat organised by an FPMT centre worldwide.",
            teacher=None, organization="FPMT",
            source_url="https://fpmt.org/centers/retreat/schedule/",
        )
        try_add(ev, known)

# ── 20. Chenrezig Institute (Australia) ──────────────────────────────────────
def scrape_chenrezig(known):
    print("\n── Chenrezig Institute ──")
    html = fetch("https://www.chenrezig.com.au/programs/")
    if not html:
        html = fetch("https://www.chenrezig.com.au/retreats/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:15]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://www.chenrezig.com.au" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Eudlo, Queensland, Australia", continent="Other",
            school="Vajrayana", etype="Retreat",
            description="Retreat or programme at Chenrezig Institute, Queensland, Australia.",
            teacher=None, organization="Chenrezig Institute",
            source_url=url,
        )
        try_add(ev, known)

# ── 21. Southern Dharma Retreat Center ───────────────────────────────────────
def scrape_southern_dharma(known):
    print("\n── Southern Dharma Retreat Center ──")
    html = fetch("https://southerndharma.org/retreat-schedule/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}\s*[-–]\s*\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:15]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://southerndharma.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Hot Springs, North Carolina, USA", continent="North America",
            school="Theravada", etype="Retreat",
            description="Insight meditation retreat at Southern Dharma Retreat Center, North Carolina.",
            teacher=None, organization="Southern Dharma Retreat Center",
            source_url=url,
        )
        try_add(ev, known)

# ── 22. Amaravati Buddhist Monastery ─────────────────────────────────────────
def scrape_amaravati(known):
    print("\n── Amaravati Buddhist Monastery ──")
    html = fetch("https://www.amaravati.org/retreat-centre/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'(\d{1,2}\s+[A-Za-z]+\s+\d{4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:15]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://www.amaravati.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Great Gaddesden, UK", continent="Europe",
            school="Theravada", etype="Retreat",
            description="Retreat at Amaravati Buddhist Monastery in the Thai Forest Tradition of Ajahn Chah.",
            teacher=None, organization="Amaravati Buddhist Monastery",
            source_url=url,
        )
        try_add(ev, known)

# ── 23. Kadampa Meditation Centre UK ─────────────────────────────────────────
def scrape_kadampa(known):
    print("\n── Kadampa (NKT-IKBU) ──")
    html = fetch("https://kadampa.org/events")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'(\d{1,2}\s+[A-Za-z]+\s+\d{4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:15]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://kadampa.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Various locations", continent="Other",
            school="Vajrayana", etype="Teachings",
            description="Teaching or festival in the New Kadampa Tradition (NKT-IKBU).",
            teacher=None, organization="Kadampa (NKT-IKBU)",
            source_url=url,
        )
        try_add(ev, known)

# ── 24. Vipassana Meditation (Dhamma.org) ────────────────────────────────────
def scrape_dhamma(known):
    print("\n── Dhamma.org (S.N. Goenka Vipassana) ──")
    # Dhamma.org lists courses by country — scrape the global schedule page
    html = fetch("https://www.dhamma.org/en/courses/search")
    if not html:
        html = fetch("https://www.dhamma.org/en-US/courses/search")
    if not html:
        return
    # Look for course entries
    entries = re.findall(
        r'(\d{4}-\d{2}-\d{2})[^<]*</[^>]+>[^<]*<[^>]+>([^<]{5,80})',
        html, re.IGNORECASE
    )
    seen = set()
    for d, title in entries[:20]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        if not future_date(d):
            continue
        ev = make_event(
            title="10-Day Vipassana Meditation Course – " + title,
            date_str=d, end_date=None,
            location="Various centres worldwide", continent="Other",
            school="Theravada", etype="Meditation",
            description="10-day silent Vipassana meditation course in the tradition of S.N. Goenka. Free of charge.",
            teacher=None, organization="Dhamma.org (S.N. Goenka)",
            source_url="https://www.dhamma.org/en/courses/search",
        )
        try_add(ev, known)

# ── 25. Sravasti Abbey ───────────────────────────────────────────────────────
def scrape_sravasti(known):
    print("\n── Sravasti Abbey ──")
    html = fetch("https://sravastiabbey.org/events/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*class="[^"]*entry-title[^"]*"[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:12]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Newport, Washington, USA", continent="North America",
            school="Mahayana", etype="Retreat",
            description="Retreat or teaching at Sravasti Abbey, a Tibetan Buddhist monastery in Washington State.",
            teacher="Venerable Thubten Chodron", organization="Sravasti Abbey",
            source_url=url,
        )
        try_add(ev, known)

# ── 26. Deer Park Monastery (Plum Village USA) ────────────────────────────────
def scrape_deer_park(known):
    print("\n── Deer Park Monastery ──")
    html = fetch("https://deerparkmonastery.org/retreats")
    if not html:
        html = fetch("https://deerparkmonastery.org/schedule")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:12]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://deerparkmonastery.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Escondido, California, USA", continent="North America",
            school="Zen", etype="Retreat",
            description="Retreat at Deer Park Monastery in the Plum Village tradition of Thich Nhat Hanh.",
            teacher="Deer Park Monastics", organization="Deer Park Monastery",
            source_url=url,
        )
        try_add(ev, known)

# ── 27. Insight Meditation Community of Washington ───────────────────────────
def scrape_imcw(known):
    print("\n── IMCW ──")
    html = fetch("https://www.imcw.org/Retreats")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:12]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://www.imcw.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Washington, DC, USA", continent="North America",
            school="Theravada", etype="Retreat",
            description="Insight meditation retreat offered by the Insight Meditation Community of Washington.",
            teacher=None, organization="IMCW",
            source_url=url,
        )
        try_add(ev, known)

# ── 28. New York Zen Center for Contemplative Care ───────────────────────────
def scrape_nyzcc(known):
    print("\n── New York Zen Center ──")
    html = fetch("https://zencare.org/programs/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:10]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://zencare.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="New York, USA", continent="North America",
            school="Zen", etype="Teachings",
            description="Programme at the New York Zen Center for Contemplative Care.",
            teacher=None, organization="NY Zen Center for Contemplative Care",
            source_url=url,
        )
        try_add(ev, known)

# ── 29. San Francisco Zen Center ─────────────────────────────────────────────
def scrape_sfzc(known):
    print("\n── San Francisco Zen Center ──")
    html = fetch("https://www.sfzc.org/programs-retreats")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:15]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://www.sfzc.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="San Francisco, California, USA", continent="North America",
            school="Zen", etype="Retreat",
            description="Programme at San Francisco Zen Center in the Soto Zen tradition.",
            teacher=None, organization="San Francisco Zen Center",
            source_url=url,
        )
        try_add(ev, known)

# ── 30. Rochester Zen Center ─────────────────────────────────────────────────
def scrape_rochester_zen(known):
    print("\n── Rochester Zen Center ──")
    html = fetch("https://rzc.org/schedule/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:12]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://rzc.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Rochester, New York, USA", continent="North America",
            school="Zen", etype="Retreat",
            description="Sesshin or programme at Rochester Zen Center in the Rinzai Zen tradition.",
            teacher=None, organization="Rochester Zen Center",
            source_url=url,
        )
        try_add(ev, known)

# ── 31. Tara Mandala Retreat Center ──────────────────────────────────────────
def scrape_tara_mandala(known):
    print("\n── Tara Mandala ──")
    html = fetch("https://taramandala.org/programs/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:12]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://taramandala.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Pagosa Springs, Colorado, USA", continent="North America",
            school="Vajrayana", etype="Retreat",
            description="Retreat at Tara Mandala Retreat Center in the Tibetan Vajrayana tradition.",
            teacher=None, organization="Tara Mandala",
            source_url=url,
        )
        try_add(ev, known)

# ── 32. Kagyu Samye Ling (Scotland) ──────────────────────────────────────────
def scrape_samye_ling(known):
    print("\n── Kagyu Samye Ling ──")
    html = fetch("https://www.samyeling.org/programme/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'(\d{1,2}\s+[A-Za-z]+\s+\d{4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:15]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://www.samyeling.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Eskdalemuir, Scotland, UK", continent="Europe",
            school="Vajrayana", etype="Retreat",
            description="Retreat or teaching at Kagyu Samye Ling, Europe's first Tibetan Buddhist centre.",
            teacher=None, organization="Kagyu Samye Ling",
            source_url=url,
        )
        try_add(ev, known)

# ── 33. Rigpa International ───────────────────────────────────────────────────
def scrape_rigpa(known):
    print("\n── Rigpa International ──")
    html = fetch("https://www.rigpa.org/events/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'(\d{1,2}\s+[A-Za-z]+\s+\d{4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:15]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://www.rigpa.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Various locations", continent="Other",
            school="Vajrayana", etype="Teachings",
            description="Teaching or retreat by Rigpa International in the Nyingma tradition of Sogyal Rinpoche.",
            teacher=None, organization="Rigpa International",
            source_url=url,
        )
        try_add(ev, known)

# ── 34. Shambhala International ───────────────────────────────────────────────
def scrape_shambhala(known):
    print("\n── Shambhala ──")
    html = fetch("https://shambhala.org/programs/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:12]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://shambhala.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Various locations", continent="Other",
            school="Vajrayana", etype="Teachings",
            description="Programme in the Shambhala Buddhist tradition founded by Chogyam Trungpa Rinpoche.",
            teacher=None, organization="Shambhala",
            source_url=url,
        )
        try_add(ev, known)

# ── 35. Thich Nhat Hanh European Institute (EIAB) ────────────────────────────
def scrape_eiab(known):
    print("\n── EIAB (European Institute of Applied Buddhism) ──")
    html = fetch("https://eiab.eu/retreats/")
    if not html:
        html = fetch("https://eiab.eu/schedule/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'(\d{1,2}\s+[A-Za-z]+\s+\d{4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:12]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://eiab.eu" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Waldbrol, Germany", continent="Europe",
            school="Zen", etype="Retreat",
            description="Retreat at the European Institute of Applied Buddhism in the Plum Village tradition.",
            teacher="Plum Village Monastics", organization="EIAB",
            source_url=url,
        )
        try_add(ev, known)

# ── 36. Dhammapadipa (European Vipassana) ────────────────────────────────────
def scrape_dhammapadipa(known):
    print("\n── Dhammapadipa (UK Vipassana) ──")
    html = fetch("https://www.dhammapadipa.dhamma.org/en/courses/")
    if not html:
        return
    entries = re.findall(
        r'(\d{4}-\d{2}-\d{2})[^<]{0,50}([^<]{5,80})',
        html, re.IGNORECASE
    )
    seen = set()
    for d, title in entries[:15]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        if not future_date(d):
            continue
        ev = make_event(
            title="10-Day Vipassana Course – Dhammapadipa " + d,
            date_str=d, end_date=None,
            location="Herefordshire, UK", continent="Europe",
            school="Theravada", etype="Meditation",
            description="10-day silent Vipassana meditation course at Dhammapadipa, UK, in the tradition of S.N. Goenka.",
            teacher=None, organization="Dhammapadipa (Dhamma.org UK)",
            source_url="https://www.dhammapadipa.dhamma.org/en/courses/",
        )
        try_add(ev, known)

# ── 37. Bodhi College ────────────────────────────────────────────────────────
def scrape_bodhi_college(known):
    print("\n── Bodhi College ──")
    html = fetch("https://bodhi.college/courses-retreats/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'(\d{1,2}\s+[A-Za-z]+\s+\d{4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:12]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://bodhi.college" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Various locations, Europe", continent="Europe",
            school="Theravada", etype="Teachings",
            description="Course or retreat at Bodhi College, offering in-depth study and practice of the early Buddhist teachings.",
            teacher=None, organization="Bodhi College",
            source_url=url,
        )
        try_add(ev, known)

# ── 38. Barre Center for Buddhist Studies ────────────────────────────────────
def scrape_bcbs(known):
    print("\n── Barre Center for Buddhist Studies ──")
    html = fetch("https://www.buddhistinquiry.org/programs/")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'([A-Za-z]+ \d{1,2}[–\-]\d{1,2},?\s+\d{4}|[A-Za-z]+ \d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:12]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://www.buddhistinquiry.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Barre, Massachusetts, USA", continent="North America",
            school="Theravada", etype="Teachings",
            description="Study programme at the Barre Center for Buddhist Studies (BCBS).",
            teacher=None, organization="Barre Center for Buddhist Studies",
            source_url=url,
        )
        try_add(ev, known)

# ── 39. Sharpham Trust (UK) ───────────────────────────────────────────────────
def scrape_sharpham(known):
    print("\n── Sharpham Trust ──")
    html = fetch("https://www.sharphamtrust.org/retreats")
    if not html:
        return
    items = re.findall(
        r'<h\d[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([^<]{5,120})</a>',
        html, re.IGNORECASE
    )
    dates = re.findall(r'(\d{1,2}\s+[A-Za-z]+\s+\d{4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})', html)
    date_idx = 0
    seen = set()
    for url, title in items[:12]:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(dates[date_idx]) if date_idx < len(dates) else None
        date_idx += 1
        if not d or not future_date(d):
            continue
        if not url.startswith("http"):
            url = "https://www.sharphamtrust.org" + url
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Devon, UK", continent="Europe",
            school="Theravada", etype="Retreat",
            description="Retreat at Sharpham Trust in Devon, UK, offering Buddhist-inspired mindfulness retreats.",
            teacher=None, organization="Sharpham Trust",
            source_url=url,
        )
        try_add(ev, known)

# ── 40. Garchen Institute – additional events ─────────────────────────────────
def scrape_garchen_extra(known):
    print("\n── Garchen Institute (extended) ──")
    html = fetch("https://garchen.net/annual-events/")
    if not html:
        return
    # Look for the detailed event blocks with specific format
    # "EventName | Date Range | Teacher | Format"
    blocks = re.findall(
        r'\*([^|*\n]{5,100})\s*\|\s*([A-Za-z]+ \d{1,2}[^|]{0,30})\s*\|',
        html, re.IGNORECASE
    )
    seen = set()
    for title, date_raw in blocks:
        title = re.sub(r'\s+', ' ', title).strip()
        if title in seen or len(title) < 5:
            continue
        seen.add(title)
        d = parse_date_str(date_raw.strip())
        if not d or not future_date(d):
            continue
        ev = make_event(
            title=title, date_str=d, end_date=None,
            location="Chino Valley, Arizona, USA", continent="North America",
            school="Vajrayana", etype="Teachings",
            description="Teaching or retreat at Garchen Buddhist Institute in the Drikung Kagyu tradition.",
            teacher="H.E. Garchen Rinpoche", organization="Garchen Buddhist Institute",
            source_url="https://garchen.net/annual-events/",
        )
        try_add(ev, known)

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    global ADDED
    print(f"Buddhist Events Scraper — {TODAY}")
    print("=" * 50)

    if not SUPA_URL or not SUPA_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY environment variables must be set.")
        return

    print("Loading existing events from database...")
    known = existing_titles()
    print(f"Found {len(known)} existing events.")

    # Run all scrapers
    scrapers = [
        # Original 18
        scrape_tushita,
        scrape_plum_village,
        scrape_gaia_house,
        scrape_spirit_rock,
        scrape_ims,
        scrape_throssel,
        scrape_upaya,
        scrape_cloud_mountain,
        scrape_garchen,
        scrape_blue_cliff,
        scrape_abhayagiri,
        scrape_drala,
        scrape_zmm,
        scrape_irc,
        scrape_mountain_cloud,
        scrape_tibethaus,
        scrape_dhagpo,
        scrape_bswa,
        # New scrapers
        scrape_fpmt,
        scrape_chenrezig,
        scrape_southern_dharma,
        scrape_amaravati,
        scrape_kadampa,
        scrape_dhamma,
        scrape_sravasti,
        scrape_deer_park,
        scrape_imcw,
        scrape_nyzcc,
        scrape_sfzc,
        scrape_rochester_zen,
        scrape_tara_mandala,
        scrape_samye_ling,
        scrape_rigpa,
        scrape_shambhala,
        scrape_eiab,
        scrape_dhammapadipa,
        scrape_bodhi_college,
        scrape_bcbs,
        scrape_sharpham,
        scrape_garchen_extra,
    ]

    for scraper in scrapers:
        try:
            scraper(known)
            time.sleep(2)  # be polite — pause between sites
        except Exception as e:
            ERRORS.append(f"{scraper.__name__}: {e}")

    print("\n" + "=" * 50)
    print(f"Done. Added {ADDED} new events.")
    if ERRORS:
        print(f"\nErrors ({len(ERRORS)}):")
        for err in ERRORS:
            print(f"  • {err}")

if __name__ == "__main__":
    main()
