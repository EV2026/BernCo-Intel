"""
Bernalillo County NM – Motivated Seller Lead Scraper
=====================================================
Scrapes the county clerk portal (Playwright) and enriches records with
parcel/owner data from the Assessor bulk DBF download (requests).

Run:  python scraper/fetch.py
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── Optional dbfread ──────────────────────────────────────────────────────────
try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False
    logging.warning("dbfread not installed – parcel enrichment disabled")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("scraper")

# ── Constants ─────────────────────────────────────────────────────────────────
CLERK_URL = (
    "https://bernalillocountynm-web.tylerhost.net/web/search/DOCSEARCH4993S1"
)
ASSESSOR_BASE = "https://assessor.bernco.gov/public.access/"
ASSESSOR_SEARCH = ASSESSOR_BASE + "search/commonsearch.aspx?mode=realprop"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]
GHL_PATH = Path("data/ghl_export.csv")

# Doc-type → (category_key, category_label)
DOC_TYPE_MAP = {
    "LP":       ("preforeclosure", "Lis Pendens"),
    "NOFC":     ("preforeclosure", "Notice of Foreclosure"),
    "TAXDEED":  ("tax",           "Tax Deed"),
    "JUD":      ("judgment",      "Judgment"),
    "CCJ":      ("judgment",      "Certified Judgment"),
    "DRJUD":    ("judgment",      "Domestic Judgment"),
    "LNCORPTX": ("lien",          "Corp Tax Lien"),
    "LNIRS":    ("lien",          "IRS Lien"),
    "LNFED":    ("lien",          "Federal Lien"),
    "LN":       ("lien",          "Lien"),
    "LNMECH":   ("lien",          "Mechanic Lien"),
    "LNHOA":    ("lien",          "HOA Lien"),
    "MEDLN":    ("lien",          "Medicaid Lien"),
    "PRO":      ("probate",       "Probate Document"),
    "NOC":      ("noc",           "Notice of Commencement"),
    "RELLP":    ("release",       "Release Lis Pendens"),
}

TARGET_DOC_TYPES = list(DOC_TYPE_MAP.keys())

# ─────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ─────────────────────────────────────────────────────────────────────────────

def retry(fn, attempts=3, delay=5):
    """Call *fn* up to *attempts* times, sleeping *delay* sec between tries."""
    last_exc = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            log.warning("Attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                time.sleep(delay)
    raise last_exc


async def retry_async(coro_fn, attempts=3, delay=5):
    last_exc = None
    for i in range(attempts):
        try:
            return await coro_fn()
        except Exception as exc:
            last_exc = exc
            log.warning("Async attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                await asyncio.sleep(delay)
    raise last_exc


def safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        cleaned = re.sub(r"[^\d.\-]", "", str(val))
        return float(cleaned) if cleaned else None
    except (ValueError, TypeError):
        return None


def parse_name_variants(full_name: str) -> list[str]:
    """Return multiple lookup variants for a full-name string."""
    name = full_name.strip().upper()
    variants = [name]
    # "LAST, FIRST" → also index "FIRST LAST" and "LAST FIRST"
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        variants.append(f"{parts[1]} {parts[0]}")   # FIRST LAST
        variants.append(f"{parts[0]} {parts[1]}")   # LAST FIRST
    else:
        tokens = name.split()
        if len(tokens) >= 2:
            variants.append(f"{tokens[-1]}, {' '.join(tokens[:-1])}")  # LAST, FIRST
            variants.append(f"{tokens[-1]} {' '.join(tokens[:-1])}")   # LAST FIRST
    return list(dict.fromkeys(variants))  # dedup, preserve order


# ─────────────────────────────────────────────────────────────────────────────
# Assessor parcel data
# ─────────────────────────────────────────────────────────────────────────────

class ParcelDB:
    """Download and index the Bernalillo County Assessor bulk DBF."""

    DOWNLOAD_URL = (
        "https://assessor.bernco.gov/public.access/search/"
        "commonsearch.aspx?mode=realprop"
    )
    # Known direct bulk-data endpoints to try first
    BULK_CANDIDATES = [
        "https://assessor.bernco.gov/public.access/download/ParcelData.zip",
        "https://assessor.bernco.gov/public.access/download/RealProp.zip",
    ]

    def __init__(self):
        self._by_owner: dict[str, list[dict]] = {}
        self._loaded = False

    # ── public ────────────────────────────────────────────────────────────────

    def load(self):
        if self._loaded:
            return
        log.info("Loading parcel data…")
        dbf_bytes = self._fetch_dbf()
        if dbf_bytes:
            self._parse_dbf(dbf_bytes)
            log.info("Parcel index built: %d owner keys", len(self._by_owner))
        else:
            log.warning("Could not obtain parcel DBF – address enrichment skipped")
        self._loaded = True

    def lookup(self, owner_name: str) -> Optional[dict]:
        """Return first matching parcel record for owner_name."""
        for variant in parse_name_variants(owner_name):
            records = self._by_owner.get(variant)
            if records:
                return records[0]
        return None

    # ── private ───────────────────────────────────────────────────────────────

    def _fetch_dbf(self) -> Optional[bytes]:
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; LeadScraper/1.0)"})

        # 1. Try direct bulk download endpoints
        for url in self.BULK_CANDIDATES:
            try:
                r = retry(lambda u=url: session.get(u, timeout=60))
                if r.status_code == 200 and len(r.content) > 1000:
                    log.info("Downloaded bulk file from %s (%d bytes)", url, len(r.content))
                    return self._extract_dbf_from_response(r.content)
            except Exception as exc:
                log.debug("Bulk candidate %s failed: %s", url, exc)

        # 2. Fall back: scrape the search page for a download link
        try:
            r = retry(lambda: session.get(self.DOWNLOAD_URL, timeout=30))
            soup = BeautifulSoup(r.text, "lxml")

            # Look for a link/button that hints at bulk download
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if any(k in href.lower() for k in ("download", "export", "bulk", ".zip", ".dbf")):
                    full = urljoin(ASSESSOR_BASE, href)
                    dr = retry(lambda u=full: session.get(u, timeout=60))
                    if dr.status_code == 200 and len(dr.content) > 1000:
                        return self._extract_dbf_from_response(dr.content)

            # 3. Try __doPostBack for export
            viewstate = soup.find("input", {"id": "__VIEWSTATE"})
            eventval = soup.find("input", {"id": "__EVENTVALIDATION"})
            if viewstate:
                payload = {
                    "__EVENTTARGET": "btnExport",
                    "__EVENTARGUMENT": "",
                    "__VIEWSTATE": viewstate.get("value", ""),
                    "__EVENTVALIDATION": eventval.get("value", "") if eventval else "",
                }
                pr = retry(lambda: session.post(self.DOWNLOAD_URL, data=payload, timeout=60))
                if pr.status_code == 200 and len(pr.content) > 1000:
                    return self._extract_dbf_from_response(pr.content)
        except Exception as exc:
            log.warning("Assessor page scrape failed: %s", exc)

        return None

    def _extract_dbf_from_response(self, content: bytes) -> Optional[bytes]:
        """Unwrap ZIP if needed; return raw DBF bytes."""
        if not HAS_DBF:
            return None
        if content[:4] == b"PK\x03\x04":  # ZIP magic
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(".dbf"):
                            log.info("Extracting %s from ZIP", name)
                            return zf.read(name)
            except Exception as exc:
                log.warning("ZIP extraction failed: %s", exc)
                return None
        return content  # assume raw DBF

    def _parse_dbf(self, dbf_bytes: bytes):
        """Parse DBF bytes and populate owner index."""
        try:
            tmp = Path("/tmp/parcels.dbf")
            tmp.write_bytes(dbf_bytes)
            table = DBF(str(tmp), ignore_missing_memofile=True)

            col = lambda row, *names: next(
                (str(row.get(n, "") or "").strip() for n in names if row.get(n)),
                ""
            )

            for row in table:
                try:
                    owner = col(row, "OWN1", "OWNER")
                    if not owner:
                        continue
                    record = {
                        "owner_raw": owner,
                        "prop_address": col(row, "SITEADDR", "SITE_ADDR"),
                        "prop_city":    col(row, "SITECITY", "SITE_CITY"),
                        "prop_state":   col(row, "SITEST", "SITE_STATE") or "NM",
                        "prop_zip":     col(row, "SITEZIP", "SITE_ZIP"),
                        "mail_address": col(row, "MAILADR1", "ADDR_1"),
                        "mail_city":    col(row, "MAILCITY", "CITY"),
                        "mail_state":   col(row, "MAILST", "STATE"),
                        "mail_zip":     col(row, "MAILZIP", "ZIP"),
                    }
                    for variant in parse_name_variants(owner):
                        self._by_owner.setdefault(variant, []).append(record)
                except Exception:
                    continue
        except Exception as exc:
            log.warning("DBF parse error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Clerk portal scraper (Playwright)
# ─────────────────────────────────────────────────────────────────────────────

class ClerkScraper:
    """Async Playwright scraper for the Tyler Technologies clerk portal."""

    def __init__(self, lookback_days: int = LOOKBACK_DAYS):
        self.lookback_days = lookback_days
        self.date_from = (datetime.now() - timedelta(days=lookback_days)).strftime("%m/%d/%Y")
        self.date_to = datetime.now().strftime("%m/%d/%Y")
        self.records: list[dict] = []

    async def run(self) -> list[dict]:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            context = await browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            )
            page = await context.new_page()
            page.set_default_timeout(45_000)

            try:
                for doc_type in TARGET_DOC_TYPES:
                    log.info("Fetching doc type: %s", doc_type)
                    try:
                        recs = await retry_async(
                            lambda dt=doc_type: self._fetch_doc_type(page, dt)
                        )
                        self.records.extend(recs)
                        log.info("  → %d records", len(recs))
                    except Exception as exc:
                        log.error("Failed fetching %s: %s", doc_type, exc)
                        continue
            finally:
                await browser.close()

        return self.records

    async def _fetch_doc_type(self, page, doc_type: str) -> list[dict]:
        await page.goto(CLERK_URL, wait_until="networkidle")
        await asyncio.sleep(1)

        # Fill date range
        try:
            await page.fill('input[name*="DateFrom"], input[id*="DateFrom"], input[placeholder*="From"]', self.date_from)
            await page.fill('input[name*="DateTo"], input[id*="DateTo"], input[placeholder*="To"]', self.date_to)
        except Exception:
            log.debug("Date field selectors may differ – trying alternatives")
            try:
                date_inputs = await page.query_selector_all('input[type="text"]')
                if len(date_inputs) >= 2:
                    await date_inputs[0].fill(self.date_from)
                    await date_inputs[1].fill(self.date_to)
            except Exception as e:
                log.warning("Could not set date range: %s", e)

        # Select document type
        try:
            # Try dropdown first
            select = await page.query_selector('select[name*="DocType"], select[id*="DocType"]')
            if select:
                await select.select_option(value=doc_type)
            else:
                # Try text input with autocomplete
                doc_input = await page.query_selector('input[name*="DocType"], input[id*="DocType"]')
                if doc_input:
                    await doc_input.fill(doc_type)
                    await asyncio.sleep(0.5)
                    # Click matching suggestion if visible
                    suggestion = await page.query_selector(f'li:has-text("{doc_type}"), div.suggestion:has-text("{doc_type}")')
                    if suggestion:
                        await suggestion.click()
        except Exception as e:
            log.warning("Doc type selection issue for %s: %s", doc_type, e)

        # Submit search
        try:
            await page.click('input[type="submit"], button[type="submit"], button:has-text("Search")')
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(1)
        except Exception as e:
            log.warning("Search submit failed for %s: %s", doc_type, e)
            return []

        # Collect all pages of results
        records = []
        page_num = 1
        while True:
            try:
                page_recs = await self._parse_results_page(page, doc_type)
                records.extend(page_recs)

                # Try to go to next page
                next_btn = await page.query_selector(
                    'a:has-text("Next"), button:has-text("Next"), '
                    'a[title="Next"], li.next > a, .pagination .next'
                )
                if not next_btn:
                    break
                is_disabled = await next_btn.get_attribute("class") or ""
                if "disabled" in is_disabled.lower():
                    break

                page_num += 1
                if page_num > 50:  # safety cap
                    break

                await next_btn.click()
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(0.8)
            except Exception as e:
                log.debug("Pagination ended: %s", e)
                break

        return records

    async def _parse_results_page(self, page, doc_type: str) -> list[dict]:
        """Parse a results table/list from the current page."""
        records = []
        content = await page.content()
        soup = BeautifulSoup(content, "lxml")
        current_url = page.url

        # Tyler Technologies portals typically render a results table
        table = soup.find("table", {"id": re.compile(r"results|search|grid", re.I)}) \
               or soup.find("table", class_=re.compile(r"results|search|grid|data", re.I)) \
               or soup.find("table")

        if not table:
            # Try list-based results
            rows = soup.find_all("div", class_=re.compile(r"result-row|doc-row|record", re.I))
            for row in rows:
                rec = self._parse_row_div(row, doc_type, current_url)
                if rec:
                    records.append(rec)
            return records

        headers = []
        header_row = table.find("tr")
        if header_row:
            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["td", "th"])
            if not cells or len(cells) < 3:
                continue
            try:
                rec = self._map_row_to_record(headers, cells, doc_type, current_url)
                if rec:
                    records.append(rec)
            except Exception as exc:
                log.debug("Row parse error: %s", exc)
                continue

        return records

    def _map_row_to_record(self, headers: list, cells, doc_type: str, base_url: str) -> Optional[dict]:
        """Map table cells to a standardised record dict."""
        def cell_text(idx):
            return cells[idx].get_text(strip=True) if idx < len(cells) else ""

        def find_col(*names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h:
                        return cell_text(i)
            return ""

        # Extract a direct link if present
        link = None
        for cell in cells:
            a = cell.find("a", href=True)
            if a:
                href = a["href"]
                if href.startswith("http"):
                    link = href
                elif href.startswith("/"):
                    link = urljoin("https://bernalillocountynm-web.tylerhost.net", href)
                break

        doc_num  = find_col("doc", "instrument", "number", "book")
        filed    = find_col("date", "filed", "recorded")
        grantor  = find_col("grantor", "owner", "from", "seller")
        grantee  = find_col("grantee", "to", "buyer")
        legal    = find_col("legal", "description", "parcel")
        amount   = find_col("amount", "consideration", "value")

        # Fallback: positional mapping for unknown headers
        if not doc_num and len(cells) >= 1:
            doc_num = cell_text(0)
        if not filed and len(cells) >= 2:
            filed = cell_text(1)
        if not grantor and len(cells) >= 3:
            grantor = cell_text(2)

        if not doc_num:
            return None

        cat, cat_label = DOC_TYPE_MAP.get(doc_type, ("other", doc_type))
        return {
            "doc_num":  doc_num,
            "doc_type": doc_type,
            "filed":    filed,
            "cat":      cat,
            "cat_label": cat_label,
            "owner":    grantor,
            "grantee":  grantee,
            "amount":   amount,
            "legal":    legal,
            "clerk_url": link or base_url,
            # Enriched later
            "prop_address": "", "prop_city": "", "prop_state": "NM", "prop_zip": "",
            "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
            "flags": [], "score": 0,
        }

    def _parse_row_div(self, div, doc_type: str, base_url: str) -> Optional[dict]:
        """Parse a div-based result row (non-table layout)."""
        text = div.get_text(" ", strip=True)
        link = None
        a = div.find("a", href=True)
        if a:
            href = a["href"]
            link = href if href.startswith("http") else urljoin(
                "https://bernalillocountynm-web.tylerhost.net", href
            )

        doc_num = ""
        m = re.search(r"\b(\d{4}-\d+|\d{6,})\b", text)
        if m:
            doc_num = m.group(1)

        if not doc_num:
            return None

        cat, cat_label = DOC_TYPE_MAP.get(doc_type, ("other", doc_type))
        return {
            "doc_num":  doc_num,
            "doc_type": doc_type,
            "filed":    "",
            "cat":      cat,
            "cat_label": cat_label,
            "owner":    "",
            "grantee":  "",
            "amount":   "",
            "legal":    text[:200],
            "clerk_url": link or base_url,
            "prop_address": "", "prop_city": "", "prop_state": "NM", "prop_zip": "",
            "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
            "flags": [], "score": 0,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Scoring & flagging
# ─────────────────────────────────────────────────────────────────────────────

def compute_flags_and_score(rec: dict, cutoff_date: datetime) -> dict:
    """Mutate rec in-place: add flags list and score int."""
    flags = []
    score = 30  # base

    doc_type = rec.get("doc_type", "")
    cat      = rec.get("cat", "")
    owner    = rec.get("owner", "").upper()
    amount   = safe_float(rec.get("amount"))

    # Filed date – new this week?
    filed_str = rec.get("filed", "")
    is_new = False
    if filed_str:
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"):
            try:
                filed_dt = datetime.strptime(filed_str.strip(), fmt)
                if filed_dt >= cutoff_date:
                    is_new = True
                break
            except ValueError:
                continue

    # Flag: Lis pendens
    if doc_type in ("LP",):
        flags.append("Lis pendens")

    # Flag: Pre-foreclosure
    if doc_type in ("LP", "NOFC"):
        flags.append("Pre-foreclosure")

    # Flag: Judgment lien
    if doc_type in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien")

    # Flag: Tax lien
    if doc_type in ("TAXDEED", "LNCORPTX", "LNIRS", "LNFED"):
        flags.append("Tax lien")

    # Flag: Mechanic lien
    if doc_type == "LNMECH":
        flags.append("Mechanic lien")

    # Flag: Probate / estate
    if doc_type == "PRO" or "ESTATE" in owner or "PROBATE" in owner:
        flags.append("Probate / estate")

    # Flag: LLC / corp owner
    if any(k in owner for k in ("LLC", "INC", "CORP", "LTD", "LP ", "L.P.", "TRUST", "L.L.C")):
        flags.append("LLC / corp owner")

    # Flag: New this week
    if is_new:
        flags.append("New this week")

    # Score adjustments
    score += len(flags) * 10

    # LP + FC combo
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20

    if amount:
        if amount > 100_000:
            score += 15
        elif amount > 50_000:
            score += 10

    if is_new:
        score += 5

    has_address = bool(rec.get("prop_address") or rec.get("mail_address"))
    if has_address:
        score += 5

    rec["flags"] = list(dict.fromkeys(flags))  # dedup
    rec["score"] = min(score, 100)
    return rec


# ─────────────────────────────────────────────────────────────────────────────
# Enrichment
# ─────────────────────────────────────────────────────────────────────────────

def enrich_records(records: list[dict], parcel_db: ParcelDB) -> list[dict]:
    enriched = 0
    for rec in records:
        owner = rec.get("owner", "").strip()
        if not owner:
            continue
        parcel = parcel_db.lookup(owner)
        if parcel:
            rec.update({
                "prop_address": parcel.get("prop_address", ""),
                "prop_city":    parcel.get("prop_city", ""),
                "prop_state":   parcel.get("prop_state", "NM"),
                "prop_zip":     parcel.get("prop_zip", ""),
                "mail_address": parcel.get("mail_address", ""),
                "mail_city":    parcel.get("mail_city", ""),
                "mail_state":   parcel.get("mail_state", ""),
                "mail_zip":     parcel.get("mail_zip", ""),
            })
            enriched += 1
    log.info("Address-enriched %d / %d records", enriched, len(records))
    return records


# ─────────────────────────────────────────────────────────────────────────────
# Output writers
# ─────────────────────────────────────────────────────────────────────────────

def build_output(records: list[dict], lookback_days: int) -> dict:
    date_to   = datetime.now().strftime("%Y-%m-%d")
    date_from = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    with_address = sum(
        1 for r in records if r.get("prop_address") or r.get("mail_address")
    )
    return {
        "fetched_at":  datetime.utcnow().isoformat() + "Z",
        "source":      "Bernalillo County Clerk – Tyler Technologies Portal",
        "date_range":  {"from": date_from, "to": date_to},
        "total":       len(records),
        "with_address": with_address,
        "records":     records,
    }


def save_json(data: dict):
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        log.info("Saved %s", path)


def save_ghl_csv(records: list[dict]):
    GHL_PATH.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]

    def split_name(full: str):
        parts = full.strip().split()
        if not parts:
            return "", ""
        if len(parts) == 1:
            return parts[0], ""
        # "LAST, FIRST …" format
        if parts[0].endswith(","):
            return " ".join(parts[1:]), parts[0].rstrip(",")
        return parts[0], " ".join(parts[1:])

    with GHL_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for rec in records:
            fname, lname = split_name(rec.get("owner", ""))
            writer.writerow({
                "First Name":             fname,
                "Last Name":              lname,
                "Mailing Address":        rec.get("mail_address", ""),
                "Mailing City":           rec.get("mail_city", ""),
                "Mailing State":          rec.get("mail_state", ""),
                "Mailing Zip":            rec.get("mail_zip", ""),
                "Property Address":       rec.get("prop_address", ""),
                "Property City":          rec.get("prop_city", ""),
                "Property State":         rec.get("prop_state", "NM"),
                "Property Zip":           rec.get("prop_zip", ""),
                "Lead Type":              rec.get("cat_label", ""),
                "Document Type":          rec.get("doc_type", ""),
                "Date Filed":             rec.get("filed", ""),
                "Document Number":        rec.get("doc_num", ""),
                "Amount/Debt Owed":       rec.get("amount", ""),
                "Seller Score":           rec.get("score", 0),
                "Motivated Seller Flags": "; ".join(rec.get("flags", [])),
                "Source":                 "Bernalillo County Clerk",
                "Public Records URL":     rec.get("clerk_url", ""),
            })
    log.info("GHL CSV saved → %s (%d rows)", GHL_PATH, len(records))


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    log.info("═" * 60)
    log.info("Bernalillo County Motivated Seller Scraper")
    log.info("Lookback: %d days  |  Doc types: %d", LOOKBACK_DAYS, len(TARGET_DOC_TYPES))
    log.info("Date range: %s → %s",
             (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y"),
             datetime.now().strftime("%m/%d/%Y"))
    log.info("═" * 60)

    cutoff_date = datetime.now() - timedelta(days=LOOKBACK_DAYS)

    # 1. Load parcel data (background – non-fatal)
    parcel_db = ParcelDB()
    try:
        parcel_db.load()
    except Exception as exc:
        log.error("Parcel DB load failed (non-fatal): %s", exc)

    # 2. Scrape clerk portal
    log.info("Starting Playwright clerk scraper…")
    scraper = ClerkScraper(lookback_days=LOOKBACK_DAYS)
    try:
        records = await scraper.run()
    except Exception as exc:
        log.error("Clerk scraper failed: %s", exc)
        records = []

    log.info("Raw records collected: %d", len(records))

    if not records:
        log.warning("No records found – writing empty output")
        save_json(build_output([], LOOKBACK_DAYS))
        return

    # 3. Deduplicate by doc_num
    seen: set[str] = set()
    deduped = []
    for r in records:
        key = r.get("doc_num", "")
        if key and key not in seen:
            seen.add(key)
            deduped.append(r)
        elif not key:
            deduped.append(r)  # keep records without doc_num
    log.info("After dedup: %d records", len(deduped))

    # 4. Enrich with parcel addresses
    deduped = enrich_records(deduped, parcel_db)

    # 5. Score & flag
    for rec in deduped:
        compute_flags_and_score(rec, cutoff_date)

    # Sort by score descending
    deduped.sort(key=lambda r: r.get("score", 0), reverse=True)

    # 6. Save outputs
    output = build_output(deduped, LOOKBACK_DAYS)
    save_json(output)
    save_ghl_csv(deduped)

    # Summary
    log.info("═" * 60)
    log.info("Done. Total: %d | With address: %d", output["total"], output["with_address"])
    score_dist = {"80-100": 0, "60-79": 0, "40-59": 0, "0-39": 0}
    for r in deduped:
        s = r.get("score", 0)
        if s >= 80:   score_dist["80-100"] += 1
        elif s >= 60: score_dist["60-79"]  += 1
        elif s >= 40: score_dist["40-59"]  += 1
        else:         score_dist["0-39"]   += 1
    log.info("Score distribution: %s", score_dist)
    log.info("═" * 60)


if __name__ == "__main__":
    asyncio.run(main())
