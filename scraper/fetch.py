"""
Bernalillo County, NM — Motivated Seller Lead Scraper  v3
==========================================================
Uses requests + BeautifulSoup against the REAL Eagle Web / RPA system:
  https://eagleweb.bernco.gov/recorder/web/

No Playwright needed — Eagle Web is plain HTML forms.

Outputs: dashboard/records.json  +  data/records.json
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import time
import traceback
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

# ── optional dbfread ────────────────────────────────────────────────────────
try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

# ── logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scraper")

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
BASE_DIR = Path(__file__).resolve().parent.parent

# Eagle Web — the REAL Bernalillo County Clerk records system
CLERK_BASE        = "https://eagleweb.bernco.gov"
CLERK_DISCLAIMER  = f"{CLERK_BASE}/recorder/web/loginPOST.jsp?guest=true"
CLERK_DOC_SEARCH  = f"{CLERK_BASE}/recorder/eagleweb/docSearch.jsp"
CLERK_DOC_RESULTS = f"{CLERK_BASE}/recorder/eagleweb/doDocSearch.jsp"

APPRAISER_URL = (
    "https://assessor.bernco.gov/public.access/search/"
    "commonsearch.aspx?mode=realprop"
)

# ── Document type mapping ────────────────────────────────────────────────────
# Keys are the codes we search for; Eagle Web may use the same or similar codes.
# We discover the actual available options from the form dynamically.
DOC_TYPE_MAP: dict[str, tuple[str, str]] = {
    "LP":       ("foreclosure", "Lis Pendens"),
    "NOFC":     ("foreclosure", "Notice of Foreclosure"),
    "TAXDEED":  ("tax",         "Tax Deed"),
    "JUD":      ("judgment",    "Judgment"),
    "CCJ":      ("judgment",    "Certified Judgment"),
    "DRJUD":    ("judgment",    "Domestic Judgment"),
    "LNCORPTX": ("lien",        "Corp Tax Lien"),
    "LNIRS":    ("lien",        "IRS Lien"),
    "LNFED":    ("lien",        "Federal Lien"),
    "LN":       ("lien",        "Lien"),
    "LNMECH":   ("lien",        "Mechanic Lien"),
    "LNHOA":    ("lien",        "HOA Lien"),
    "MEDLN":    ("lien",        "Medicaid Lien"),
    "PRO":      ("probate",     "Probate"),
    "NOC":      ("other",       "Notice of Commencement"),
    "RELLP":    ("release",     "Release Lis Pendens"),
}

# Keywords to match against Eagle Web's dropdown option text
# so we can find the right option value even if codes differ
DOC_KEYWORDS: dict[str, list[str]] = {
    "LP":       ["lis pendens", "lispendens", "lp"],
    "NOFC":     ["notice of foreclosure", "foreclosure", "nofc"],
    "TAXDEED":  ["tax deed", "taxdeed"],
    "JUD":      ["judgment", "judgement", "jud"],
    "CCJ":      ["certified judgment", "ccj"],
    "DRJUD":    ["domestic", "drjud"],
    "LNCORPTX": ["corp tax", "corporate tax", "lncorptx"],
    "LNIRS":    ["irs lien", "irs", "lnirs"],
    "LNFED":    ["federal lien", "federal", "lnfed"],
    "LN":       ["lien"],
    "LNMECH":   ["mechanic", "mechanics", "lnmech"],
    "LNHOA":    ["hoa", "homeowner", "lnhoa"],
    "MEDLN":    ["medicaid", "medln"],
    "PRO":      ["probate", "pro"],
    "NOC":      ["notice of commencement", "commencement", "noc"],
    "RELLP":    ["release lis pendens", "release lp", "rellp"],
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def retry(fn, attempts: int = 3, delay: float = 3.0):
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                time.sleep(delay * (i + 1))
    raise RuntimeError(f"All {attempts} attempts failed")


def clean(s: Any) -> str:
    if s is None:
        return ""
    return " ".join(str(s).split()).strip()


def parse_amount(raw: str) -> float | None:
    if not raw:
        return None
    cleaned = re.sub(r"[^\d.]", "", raw.replace(",", ""))
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def normalise_date(raw: str) -> str:
    if not raw:
        return ""
    raw = clean(raw)
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw


def name_variants(full: str) -> list[str]:
    full = full.upper().strip()
    variants = {full}
    suffixed = re.sub(
        r"\b(LLC|INC|CORP|TRUST|LTD|LP|LLP|ETUX|ETAL)\b", "", full
    ).strip(" ,")
    variants.add(suffixed)
    if "," in full:
        parts = [p.strip() for p in full.split(",", 1)]
        variants.add(" ".join(parts))
        if len(parts) > 1:
            variants.add(f"{parts[1]} {parts[0]}")
    else:
        words = full.split()
        if len(words) == 2:
            variants.add(f"{words[1]}, {words[0]}")
            variants.add(f"{words[1]} {words[0]}")
    return [v for v in variants if v]


def score_record(rec: dict) -> tuple[int, list[str]]:
    flags: list[str] = []
    score = 30
    cat      = rec.get("cat", "")
    doc_type = rec.get("doc_type", "")
    amount   = rec.get("amount")
    filed    = rec.get("filed", "")
    owner    = rec.get("owner", "")

    if doc_type in ("LP", "NOFC"):
        flags.append("Lis pendens" if doc_type == "LP" else "Pre-foreclosure")
    if cat == "judgment":
        flags.append("Judgment lien")
    if doc_type in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"):
        flags.append("Tax lien")
    if doc_type == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "probate":
        flags.append("Probate / estate")
    if re.search(r"\b(LLC|INC|CORP|LTD|LP|LLP)\b", owner, re.I):
        flags.append("LLC / corp owner")

    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).date()
    try:
        if filed and datetime.strptime(filed[:10], "%Y-%m-%d").date() >= cutoff:
            flags.append("New this week")
    except Exception:
        pass

    score += 10 * len(flags)
    if "Lis pendens" in flags or "Pre-foreclosure" in flags:
        score += 20
    if amount:
        if amount > 100_000:
            score += 15
        elif amount > 50_000:
            score += 10
    if "New this week" in flags:
        score += 5
    if rec.get("prop_address"):
        score += 5

    return min(score, 100), flags


# ═══════════════════════════════════════════════════════════════════════════
# EAGLE WEB SESSION  (requests + BeautifulSoup)
# ═══════════════════════════════════════════════════════════════════════════

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def accept_disclaimer(session: requests.Session) -> bool:
    """
    GET the disclaimer page then POST to accept it.
    Eagle Web sets a session cookie that authorises subsequent requests.
    Returns True if we end up on a non-disclaimer page.
    """
    log.info("Accepting Eagle Web disclaimer…")
    try:
        # Step 1: GET disclaimer to pick up session cookies + any hidden fields
        r = retry(lambda: session.get(CLERK_DISCLAIMER, timeout=20))
        soup = BeautifulSoup(r.text, "lxml")

        # Log what buttons/inputs are on the page
        btns = [(t.name, t.get("type",""), t.get("value",""), t.get_text(strip=True)[:40])
                for t in soup.find_all(["input", "button", "a"])
                if t.get("type") not in ("hidden",)]
        log.info("Disclaimer page elements: %s", btns[:20])

        # Harvest hidden fields (ASP.NET viewstate etc.)
        hidden = {
            inp["name"]: inp.get("value", "")
            for inp in soup.find_all("input", {"type": "hidden"})
            if inp.get("name")
        }

        # Step 2: POST to same URL mimicking "I Accept" button click
        # Eagle Web typically accepts these field combinations:
        post_payloads = [
            {**hidden, "disclaimer": "true"},
            {**hidden, "action": "accept"},
            {**hidden, "accept": "I Accept"},
            {**hidden, "AcceptDisclaimer": "true"},
            hidden,   # just submit the hidden fields
        ]

        for payload in post_payloads:
            r2 = retry(lambda p=payload: session.post(
                CLERK_DISCLAIMER, data=p, timeout=20,
                allow_redirects=True
            ))
            if "disclaimer" not in r2.url.lower() and len(r2.text) > 500:
                log.info("Disclaimer accepted (POST payload worked) → %s", r2.url)
                return True

        # Step 3: Try GET on the doc search page directly
        # (some Eagle Web installs don't require the disclaimer POST)
        r3 = retry(lambda: session.get(CLERK_DOC_SEARCH, timeout=20))
        if r3.status_code == 200 and "search" in r3.text.lower():
            log.info("Disclaimer bypassed — doc search accessible directly")
            return True

        # Step 4: Try the direct guest login URL pattern
        for url in [
            f"{CLERK_BASE}/recorder/web/loginPOST.jsp",
            f"{CLERK_BASE}/recorder/eagleweb/loginPOST.jsp",
        ]:
            try:
                r4 = retry(lambda u=url: session.post(
                    u,
                    data={**hidden, "disclaimer": "true", "guest": "true"},
                    timeout=20,
                    allow_redirects=True
                ))
                if r4.status_code == 200 and "search" in r4.text.lower():
                    log.info("Disclaimer accepted via %s", url)
                    return True
            except Exception:
                pass

        log.warning("Could not definitively accept disclaimer — will try searching anyway")
        return False

    except Exception:
        log.warning("Disclaimer error:\n%s", traceback.format_exc())
        return False


def discover_doc_types(session: requests.Session) -> dict[str, str]:
    """
    Fetch the Eagle Web doc search form and return a mapping of
    our internal doc type codes → the option values Eagle Web expects.
    """
    log.info("Discovering available doc type codes from Eagle Web form…")
    option_map: dict[str, str] = {}   # our_code → eagle_value

    try:
        r = retry(lambda: session.get(CLERK_DOC_SEARCH, timeout=20))
        soup = BeautifulSoup(r.text, "lxml")

        # Find ALL <select> elements and their options
        for sel in soup.find_all("select"):
            sel_name = sel.get("name", "").lower()
            opts = [(o.get("value", ""), o.get_text(strip=True))
                    for o in sel.find_all("option")]
            if opts:
                log.info("  Select '%s' — %d options: %s",
                         sel_name, len(opts), opts[:8])

            # Is this the doc type selector?
            if any(kw in sel_name for kw in
                   ("doctype", "type", "instrument", "rectype", "doctypes")):
                for val, text in opts:
                    if not val:
                        continue
                    text_l = text.lower()
                    for our_code, keywords in DOC_KEYWORDS.items():
                        if any(kw in text_l for kw in keywords):
                            if our_code not in option_map:
                                option_map[our_code] = val
                                log.info("    Mapped %s → '%s' ('%s')",
                                         our_code, val, text)
                            break
                    # Also try exact code match
                    val_up = val.upper().strip()
                    if val_up in DOC_TYPE_MAP and val_up not in option_map:
                        option_map[val_up] = val

    except Exception:
        log.warning("Could not discover doc types:\n%s", traceback.format_exc())

    # Fallback: use our own codes directly (Eagle Web often accepts them)
    for code in DOC_TYPE_MAP:
        if code not in option_map:
            option_map[code] = code   # try the raw code

    log.info("Doc type map: %d entries", len(option_map))
    return option_map


def get_form_meta(session: requests.Session, url: str) -> dict:
    """Fetch a page and return all hidden field values + select options."""
    try:
        r = retry(lambda: session.get(url, timeout=20))
        soup = BeautifulSoup(r.text, "lxml")
        hidden = {
            inp["name"]: inp.get("value", "")
            for inp in soup.find_all("input", {"type": "hidden"})
            if inp.get("name")
        }
        return hidden
    except Exception:
        return {}


def search_doc_type(
    session: requests.Session,
    doc_type_code: str,
    eagle_value: str,
    date_from: str,  # MM/DD/YYYY
    date_to: str,
    form_hidden: dict,
) -> list[dict]:
    """
    POST to Eagle Web doDocSearch.jsp for one doc type.
    Returns a list of raw record dicts.
    """
    results: list[dict] = []

    # Eagle Web common field name variants for doc type, date from, date to
    # We'll try a few known patterns
    post_bodies = [
        # Pattern 1 — most common Eagle Web
        {
            **form_hidden,
            "searchType":        "Document",
            "recordedDateFrom":  date_from,
            "recordedDateTo":    date_to,
            "doctypes":          eagle_value,
            "submitButton":      "Search",
        },
        # Pattern 2 — alternate field names
        {
            **form_hidden,
            "searchType":        "Document",
            "dateFrom":          date_from,
            "dateTo":            date_to,
            "docType":           eagle_value,
            "Submit":            "Search",
        },
        # Pattern 3 — some installs use recType
        {
            **form_hidden,
            "searchType":        "Document",
            "recordedDateFrom":  date_from,
            "recordedDateTo":    date_to,
            "recType":           eagle_value,
            "submitButton":      "Search",
        },
    ]

    for attempt, body in enumerate(post_bodies):
        try:
            log.info("  Searching %s (pattern %d)…", doc_type_code, attempt + 1)
            r = retry(lambda b=body: session.post(
                CLERK_DOC_RESULTS, data=b, timeout=30, allow_redirects=True
            ))

            soup = BeautifulSoup(r.text, "lxml")
            log.info("    Response URL: %s  |  page title: %s",
                     r.url, soup.title.string if soup.title else "?")

            rows = parse_results_page(soup, doc_type_code, r.url)
            if rows:
                log.info("    Found %d records on attempt %d", len(rows), attempt + 1)

            # Paginate
            all_rows = list(rows)
            page_num = 1
            while True:
                next_url = find_next_page(soup, r.url)
                if not next_url or page_num > 50:
                    break
                page_num += 1
                r = retry(lambda u=next_url: session.get(u, timeout=20))
                soup = BeautifulSoup(r.text, "lxml")
                more = parse_results_page(soup, doc_type_code, r.url)
                all_rows.extend(more)
                log.info("    Page %d: %d more rows", page_num, len(more))
                if not more:
                    break

            if all_rows:
                results = all_rows
                break   # found results with this pattern

        except Exception:
            log.warning("    Search attempt %d failed:\n%s",
                        attempt + 1, traceback.format_exc())

    log.info("  %s → %d records total", doc_type_code, len(results))
    return results


def parse_results_page(
    soup: BeautifulSoup, doc_type: str, current_url: str
) -> list[dict]:
    """
    Parse one Eagle Web results page.
    Eagle Web renders results in an HTML table.
    """
    rows_out: list[dict] = []

    # ── find the results table ──────────────────────────────────────────
    tables = soup.find_all("table")
    for table in tables:
        headers_raw = [th.get_text(strip=True).lower()
                       for th in table.find_all("th")]
        if not headers_raw:
            first_tr = table.find("tr")
            if first_tr:
                headers_raw = [td.get_text(strip=True).lower()
                               for td in first_tr.find_all("td")]

        # Must look like a document results table
        relevant = {"instrument", "doc", "type", "grantor", "grantee",
                    "recorded", "date", "book", "page", "legal"}
        if not any(any(rk in h for rk in relevant) for h in headers_raw):
            continue

        col = {h: i for i, h in enumerate(headers_raw)}

        def gcol(*names: str) -> int | None:
            for n in names:
                for h, i in col.items():
                    if n in h:
                        return i
            return None

        idx_num     = gcol("instrument", "doc number", "doc num",
                           "document number", "doc #", "number", "inst #")
        idx_type    = gcol("doc type", "type", "instrument type", "rectype")
        idx_grantor = gcol("grantor", "owner", "seller")
        idx_grantee = gcol("grantee", "buyer")
        idx_date    = gcol("recorded date", "date recorded",
                           "recorded", "date filed", "filed date", "date")
        idx_legal   = gcol("legal", "description", "legal desc")
        idx_amount  = gcol("amount", "consideration", "value")

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if not cells:
                continue

            def cell(idx: int | None) -> str:
                if idx is None or idx >= len(cells):
                    return ""
                return clean(cells[idx].get_text(strip=True))

            doc_num  = cell(idx_num)
            filed    = cell(idx_date)
            grantor  = cell(idx_grantor)
            grantee  = cell(idx_grantee)
            legal    = cell(idx_legal)
            amount_r = cell(idx_amount)
            row_type = cell(idx_type) or doc_type

            if not doc_num and not filed:
                continue

            # Build direct clerk URL from any link in the row
            link_tag = tr.find("a", href=True)
            if link_tag:
                href = link_tag["href"]
                if href.startswith("http"):
                    clerk_url = href
                elif href.startswith("/"):
                    clerk_url = CLERK_BASE + href
                else:
                    clerk_url = CLERK_BASE + "/recorder/eagleweb/" + href.lstrip("/")
            else:
                clerk_url = current_url

            rows_out.append({
                "doc_num":   doc_num,
                "doc_type":  row_type.upper().strip() or doc_type,
                "filed":     normalise_date(filed),
                "owner":     grantor,
                "grantee":   grantee,
                "amount":    parse_amount(amount_r),
                "legal":     legal,
                "clerk_url": clerk_url,
            })

    # ── fallback: look for any div/span pattern ──────────────────────────
    if not rows_out:
        for div in soup.find_all(["div", "tr"],
                                  class_=re.compile(r"result|row|record|item", re.I)):
            text  = div.get_text(" ", strip=True)
            doc_m = re.search(
                r"(?:Instrument|Doc(?:ument)?)\s*(?:No|#|Number)[:\s]*([A-Z0-9\-]+)",
                text, re.I
            )
            date_m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", text)
            if not (doc_m or date_m):
                continue
            link_tag  = div.find("a", href=True)
            href      = link_tag["href"] if link_tag else ""
            clerk_url = (CLERK_BASE + href) if href.startswith("/") else (href or current_url)
            rows_out.append({
                "doc_num":   doc_m.group(1) if doc_m else "",
                "doc_type":  doc_type,
                "filed":     normalise_date(date_m.group(1) if date_m else ""),
                "owner":     "", "grantee": "", "amount": None, "legal": "",
                "clerk_url": clerk_url,
            })

    return rows_out


def find_next_page(soup: BeautifulSoup, current_url: str) -> str | None:
    """Return URL of next results page, or None."""
    for tag in soup.find_all(["a", "input", "button"]):
        txt = (tag.get_text(strip=True) + tag.get("value", "")).lower()
        if txt.strip() in ("next", "next >", ">", ">>", "next page"):
            href = tag.get("href", "")
            if href:
                if href.startswith("http"):
                    return href
                if href.startswith("/"):
                    return CLERK_BASE + href
                return CLERK_BASE + "/recorder/eagleweb/" + href.lstrip("/")
            # doPostBack style
            onclick = tag.get("onclick", "")
            m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", onclick)
            if m:
                return current_url + f"?__EVENTTARGET={m.group(1)}&__EVENTARGUMENT={m.group(2)}"
    return None


# ═══════════════════════════════════════════════════════════════════════════
# PARCEL / APPRAISER DATA
# ═══════════════════════════════════════════════════════════════════════════

def download_parcel_dbf(session: requests.Session) -> dict[str, dict]:
    log.info("Downloading parcel bulk data from assessor…")
    owner_index: dict[str, dict] = {}
    try:
        resp = retry(lambda: session.get(APPRAISER_URL, timeout=30))
        soup = BeautifulSoup(resp.text, "lxml")

        vs  = soup.find("input", {"id": "__VIEWSTATE"})
        evv = soup.find("input", {"id": "__EVENTVALIDATION"})
        vsg = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})

        viewstate    = vs["value"]  if vs  else ""
        evval        = evv["value"] if evv else ""
        viewstategen = vsg["value"] if vsg else ""

        download_targets = []
        for tag in soup.find_all(["a", "input", "button"]):
            text = (tag.get_text() + tag.get("value","") + tag.get("onclick","")).lower()
            if any(kw in text for kw in ("download","export","bulk","dbf","zip")):
                download_targets.append(tag)

        payload = {
            "__VIEWSTATE":          viewstate,
            "__EVENTVALIDATION":    evval,
            "__VIEWSTATEGENERATOR": viewstategen,
        }

        dbf_bytes: bytes | None = None
        for tag in download_targets:
            onclick = tag.get("onclick","")
            href    = tag.get("href","")
            m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", onclick)
            if m:
                payload["__EVENTTARGET"]   = m.group(1)
                payload["__EVENTARGUMENT"] = m.group(2)
                r2 = retry(lambda: session.post(APPRAISER_URL, data=payload, timeout=60))
                ctype = r2.headers.get("content-type","")
                if "zip" in ctype or "octet" in ctype or len(r2.content) > 50_000:
                    dbf_bytes = r2.content
                    log.info("Got bulk file via doPostBack (%d bytes)", len(dbf_bytes))
                    break
            elif href and href.lower().endswith((".zip",".dbf")):
                url = href if href.startswith("http") else APPRAISER_URL.rsplit("/",1)[0]+"/"+href.lstrip("/")
                r2 = retry(lambda: session.get(url, timeout=60))
                dbf_bytes = r2.content
                log.info("Got bulk file via direct link (%d bytes)", len(dbf_bytes))
                break

        if dbf_bytes is None:
            log.warning("Could not locate bulk parcel download — owner lookups will be empty")
            return owner_index

        if not HAS_DBF:
            log.warning("dbfread not installed — skipping parcel parse")
            return owner_index

        raw_dbf: bytes | None = None
        try:
            with zipfile.ZipFile(io.BytesIO(dbf_bytes)) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".dbf"):
                        raw_dbf = zf.read(name)
                        log.info("Extracted %s from ZIP", name)
                        break
        except zipfile.BadZipFile:
            raw_dbf = dbf_bytes

        if raw_dbf is None:
            log.warning("No DBF found inside downloaded file")
            return owner_index

        tmp = Path("/tmp/parcels.dbf")
        tmp.write_bytes(raw_dbf)
        table  = DBF(str(tmp), load=True, ignore_missing_memofile=True)
        fields = [f.name.upper() for f in table.fields]
        log.info("DBF fields: %s", fields)

        def col(row: dict, *candidates: str) -> str:
            for c in candidates:
                v = row.get(c) or row.get(c.lower())
                if v:
                    return clean(v)
            return ""

        for row in table:
            row   = {k.upper(): v for k, v in row.items()}
            owner = col(row, "OWNER","OWN1","OWN_NAME")
            if not owner:
                continue
            addr_info = {
                "prop_address": col(row, "SITE_ADDR","SITEADDR","SITE_ADDRESS"),
                "prop_city":    col(row, "SITE_CITY","SITECITY"),
                "prop_state":   col(row, "SITE_STATE") or "NM",
                "prop_zip":     col(row, "SITE_ZIP","SITEZIP"),
                "mail_address": col(row, "ADDR_1","MAILADR1","MAIL_ADDR"),
                "mail_city":    col(row, "CITY","MAILCITY","MAIL_CITY"),
                "mail_state":   col(row, "STATE","MAIL_STATE") or "NM",
                "mail_zip":     col(row, "ZIP","MAILZIP","MAIL_ZIP"),
            }
            for variant in name_variants(owner):
                owner_index[variant] = addr_info

        log.info("Parcel index built: %d owner entries", len(owner_index))

    except Exception:
        log.warning("Parcel download failed:\n%s", traceback.format_exc())

    return owner_index


def lookup_owner(name: str, index: dict[str, dict]) -> dict:
    for variant in name_variants(name):
        if variant in index:
            return index[variant]
    return {}


# ═══════════════════════════════════════════════════════════════════════════
# CLERK SCRAPE ORCHESTRATION
# ═══════════════════════════════════════════════════════════════════════════

def run_clerk_scrape(date_from: str, date_to: str) -> list[dict]:
    """
    Scrape Eagle Web for all target doc types using requests.
    date_from / date_to: MM/DD/YYYY
    """
    all_rows: list[dict] = []
    session = make_session()

    # 1. Accept disclaimer
    accept_disclaimer(session)

    # 2. Discover actual option values for doc types from the search form
    doc_type_options = discover_doc_types(session)

    # 3. Grab any hidden form fields needed for POST
    form_hidden = get_form_meta(session, CLERK_DOC_SEARCH)
    log.info("Form hidden fields: %s", list(form_hidden.keys())[:10])

    # 4. Search for each doc type
    for our_code in DOC_TYPE_MAP:
        eagle_val = doc_type_options.get(our_code, our_code)
        try:
            rows = retry(
                lambda c=our_code, v=eagle_val: search_doc_type(
                    session, c, v, date_from, date_to, form_hidden
                ),
                attempts=2, delay=3.0,
            )
            all_rows.extend(rows)
        except Exception:
            log.error("Failed searching %s:\n%s", our_code, traceback.format_exc())

    log.info("Clerk scrape complete — %d raw records", len(all_rows))
    return all_rows


# ═══════════════════════════════════════════════════════════════════════════
# BUILD FINAL RECORDS
# ═══════════════════════════════════════════════════════════════════════════

def build_records(
    raw: list[dict],
    parcel_index: dict[str, dict],
    date_from_iso: str,
    date_to_iso: str,
) -> dict:
    cutoff = datetime.fromisoformat(date_from_iso).date()
    to_dt  = datetime.fromisoformat(date_to_iso).date()

    records: list[dict] = []
    seen: set[str] = set()

    for raw_r in raw:
        try:
            doc_num   = clean(raw_r.get("doc_num",   ""))
            doc_type  = clean(raw_r.get("doc_type",  "")).upper()
            filed     = clean(raw_r.get("filed",     ""))
            owner     = clean(raw_r.get("owner",     ""))
            grantee   = clean(raw_r.get("grantee",   ""))
            amount    = raw_r.get("amount")
            legal     = clean(raw_r.get("legal",     ""))
            clerk_url = clean(raw_r.get("clerk_url", ""))

            key = f"{doc_num}::{doc_type}"
            if key in seen:
                continue
            seen.add(key)

            if filed:
                try:
                    filed_dt = datetime.strptime(filed[:10], "%Y-%m-%d").date()
                    if not (cutoff <= filed_dt <= to_dt):
                        continue
                except ValueError:
                    pass

            cat, cat_label = DOC_TYPE_MAP.get(doc_type, ("other", doc_type))
            parcel = lookup_owner(owner, parcel_index) if owner else {}

            rec: dict = {
                "doc_num":      doc_num,
                "doc_type":     doc_type,
                "filed":        filed,
                "cat":          cat,
                "cat_label":    cat_label,
                "owner":        owner,
                "grantee":      grantee,
                "amount":       amount,
                "legal":        legal,
                "prop_address": parcel.get("prop_address", ""),
                "prop_city":    parcel.get("prop_city",    ""),
                "prop_state":   parcel.get("prop_state",   "NM"),
                "prop_zip":     parcel.get("prop_zip",     ""),
                "mail_address": parcel.get("mail_address", ""),
                "mail_city":    parcel.get("mail_city",    ""),
                "mail_state":   parcel.get("mail_state",   "NM"),
                "mail_zip":     parcel.get("mail_zip",     ""),
                "clerk_url":    clerk_url,
                "flags":        [],
                "score":        0,
            }

            score, flags = score_record(rec)
            rec["score"] = score
            rec["flags"] = flags
            records.append(rec)

        except Exception:
            log.warning("Skipping bad record: %s\n%s", raw_r, traceback.format_exc())

    records.sort(key=lambda r: -r["score"])
    with_addr = sum(1 for r in records if r.get("prop_address"))

    return {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "source":       "Bernalillo County Clerk — Eagle Web / RPA",
        "date_range":   f"{date_from_iso} to {date_to_iso}",
        "total":        len(records),
        "with_address": with_addr,
        "records":      records,
    }


# ═══════════════════════════════════════════════════════════════════════════
# GHL CSV EXPORT
# ═══════════════════════════════════════════════════════════════════════════

def export_ghl_csv(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "First Name","Last Name",
        "Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number",
        "Amount/Debt Owed","Seller Score","Motivated Seller Flags",
        "Source","Public Records URL",
    ]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for r in records:
            owner  = r.get("owner","")
            parts  = owner.replace(","," ").split()
            first  = parts[-1] if len(parts) > 1 else ""
            last   = parts[0]  if parts else owner
            writer.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        r.get("mail_address",""),
                "Mailing City":           r.get("mail_city",   ""),
                "Mailing State":          r.get("mail_state",  ""),
                "Mailing Zip":            r.get("mail_zip",    ""),
                "Property Address":       r.get("prop_address",""),
                "Property City":          r.get("prop_city",   ""),
                "Property State":         r.get("prop_state",  ""),
                "Property Zip":           r.get("prop_zip",    ""),
                "Lead Type":              r.get("cat_label",   ""),
                "Document Type":          r.get("doc_type",    ""),
                "Date Filed":             r.get("filed",       ""),
                "Document Number":        r.get("doc_num",     ""),
                "Amount/Debt Owed":       r.get("amount",      ""),
                "Seller Score":           r.get("score",       ""),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source":                 "Bernalillo County Clerk — Eagle Web",
                "Public Records URL":     r.get("clerk_url",   ""),
            })
    log.info("GHL CSV written → %s (%d rows)", path, len(records))


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    log.info("=" * 60)
    log.info("Bernalillo County Motivated Seller Scraper  v3")
    log.info("Target: Eagle Web / RPA  (%s)", CLERK_BASE)
    log.info("Lookback: %d days", LOOKBACK_DAYS)

    now       = datetime.now(timezone.utc)
    date_to   = now.date()
    date_from = (now - timedelta(days=LOOKBACK_DAYS)).date()

    date_from_iso = date_from.isoformat()
    date_to_iso   = date_to.isoformat()
    date_from_str = date_from.strftime("%m/%d/%Y")
    date_to_str   = date_to.strftime("%m/%d/%Y")

    log.info("Date range: %s → %s", date_from_iso, date_to_iso)

    # Shared requests session (used for both clerk + assessor)
    session = make_session()

    # 1. Parcel index
    parcel_index = download_parcel_dbf(session)

    # 2. Clerk scrape (plain HTTP — no Playwright)
    raw_records = run_clerk_scrape(date_from_str, date_to_str)

    # 3. Build output
    output = build_records(raw_records, parcel_index, date_from_iso, date_to_iso)
    log.info("Final: %d records  (%d with address)",
             output["total"], output["with_address"])

    # 4. Write JSON
    for dest_path in [
        BASE_DIR / "dashboard" / "records.json",
        BASE_DIR / "data"      / "records.json",
    ]:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_text(
            json.dumps(output, indent=2, default=str), encoding="utf-8"
        )
        log.info("Written → %s", dest_path)

    # 5. GHL CSV
    today_str = now.strftime("%Y%m%d")
    export_ghl_csv(
        output["records"],
        BASE_DIR / "data" / f"ghl_export_{today_str}.csv"
    )

    log.info("=" * 60)
    log.info("Scrape complete ✓")


if __name__ == "__main__":
    main()
