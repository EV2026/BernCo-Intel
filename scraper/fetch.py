"""
Bernalillo County, NM — Motivated Seller Lead Scraper
======================================================
Playwright (async) for clerk portal  ·  requests+BS4 for appraiser
Outputs: dashboard/records.json  +  data/records.json
v2 — disclaimer handling + debug screenshots
"""

from __future__ import annotations

import asyncio
import base64
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
from playwright.async_api import async_playwright, Page, BrowserContext

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
BASE_DIR = Path(__file__).resolve().parent.parent          # repo root

CLERK_BASE = "https://bernalillocountynm-web.tylerhost.net"
CLERK_SEARCH = (
    f"{CLERK_BASE}/web/search/DOCSEARCH4993S1"
)

APPRAISER_URL = (
    "https://assessor.bernco.gov/public.access/search/"
    "commonsearch.aspx?mode=realprop"
)

# doc-type → (category code, human label)
DOC_TYPE_MAP: dict[str, tuple[str, str]] = {
    "LP":       ("foreclosure",  "Lis Pendens"),
    "NOFC":     ("foreclosure",  "Notice of Foreclosure"),
    "TAXDEED":  ("tax",          "Tax Deed"),
    "JUD":      ("judgment",     "Judgment"),
    "CCJ":      ("judgment",     "Certified Judgment"),
    "DRJUD":    ("judgment",     "Domestic Judgment"),
    "LNCORPTX": ("lien",         "Corp Tax Lien"),
    "LNIRS":    ("lien",         "IRS Lien"),
    "LNFED":    ("lien",         "Federal Lien"),
    "LN":       ("lien",         "Lien"),
    "LNMECH":   ("lien",         "Mechanic Lien"),
    "LNHOA":    ("lien",         "HOA Lien"),
    "MEDLN":    ("lien",         "Medicaid Lien"),
    "PRO":      ("probate",      "Probate"),
    "NOC":      ("other",        "Notice of Commencement"),
    "RELLP":    ("release",      "Release Lis Pendens"),
}

TARGET_TYPES = list(DOC_TYPE_MAP.keys())

# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def retry(fn, attempts: int = 3, delay: float = 3.0):
    """Synchronous retry wrapper."""
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                time.sleep(delay * (i + 1))
    raise RuntimeError(f"All {attempts} attempts failed")


async def async_retry(coro_fn, attempts: int = 3, delay: float = 3.0):
    """Async retry wrapper."""
    for i in range(attempts):
        try:
            return await coro_fn()
        except Exception as exc:
            log.warning("Async attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                await asyncio.sleep(delay * (i + 1))
    raise RuntimeError(f"All {attempts} async attempts failed")


def parse_amount(raw: str) -> float | None:
    """Extract a numeric dollar amount from a messy string."""
    if not raw:
        return None
    cleaned = re.sub(r"[^\d.]", "", raw.replace(",", ""))
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def clean(s: Any) -> str:
    return str(s).strip() if s else ""


def name_variants(full: str) -> list[str]:
    """
    Generate lookup variants for owner name matching.
    Input  : "DOE JOHN" or "DOE, JOHN" or "JOHN DOE"
    Returns: list of normalised variants to check against parcel data
    """
    full = full.upper().strip()
    variants = {full}

    # strip common suffixes for matching
    suffixed = re.sub(r"\b(LLC|INC|CORP|TRUST|LTD|LP|LLP|ETUX|ETAL)\b", "", full).strip(" ,")
    variants.add(suffixed)

    # "LAST, FIRST" → also add "LAST FIRST" and "FIRST LAST"
    if "," in full:
        parts = [p.strip() for p in full.split(",", 1)]
        variants.add(" ".join(parts))
        variants.add(f"{parts[1]} {parts[0]}")
    else:
        words = full.split()
        if len(words) == 2:
            variants.add(f"{words[1]}, {words[0]}")
            variants.add(f"{words[1]} {words[0]}")

    return [v for v in variants if v]


def score_record(rec: dict) -> tuple[int, list[str]]:
    """
    Compute seller score 0-100 and return (score, flags).
    """
    flags: list[str] = []
    score = 30  # base

    cat = rec.get("cat", "")
    doc_type = rec.get("doc_type", "")
    amount = rec.get("amount")
    filed = rec.get("filed", "")
    owner = rec.get("owner", "")

    # ── flag detection ───────────────────────────────────────────────────
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
        filed_dt = datetime.strptime(filed[:10], "%Y-%m-%d").date()
        if filed_dt >= cutoff:
            flags.append("New this week")
    except Exception:
        pass

    # ── scoring ──────────────────────────────────────────────────────────
    score += 10 * len(flags)

    # LP + foreclosure combo
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
# PARCEL / APPRAISER DATA
# ═══════════════════════════════════════════════════════════════════════════

def download_parcel_dbf(session: requests.Session) -> dict[str, dict]:
    """
    Download the bulk parcel DBF from the Bernalillo County Assessor.
    Returns a dict keyed by normalised owner name → address info.
    Falls back to an empty dict on any failure.
    """
    log.info("Downloading parcel bulk data from assessor …")
    owner_index: dict[str, dict] = {}

    try:
        # ── Step 1: GET the search page to harvest ASP.NET viewstate ────
        resp = retry(lambda: session.get(APPRAISER_URL, timeout=30))
        soup = BeautifulSoup(resp.text, "lxml")

        vs = soup.find("input", {"id": "__VIEWSTATE"})
        evv = soup.find("input", {"id": "__EVENTVALIDATION"})
        vsg = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})

        viewstate   = vs["value"]  if vs  else ""
        evval       = evv["value"] if evv else ""
        viewstategen = vsg["value"] if vsg else ""

        # ── Step 2: look for a "download" or "export" link/button ───────
        # Tyler / NIC portals often have a doPostBack to generate a ZIP/DBF
        download_targets = []
        for tag in soup.find_all(["a", "input", "button"]):
            text = (tag.get_text() + tag.get("value", "") + tag.get("onclick", "")).lower()
            if any(kw in text for kw in ("download", "export", "bulk", "dbf", "zip")):
                download_targets.append(tag)

        payload = {
            "__VIEWSTATE": viewstate,
            "__EVENTVALIDATION": evval,
            "__VIEWSTATEGENERATOR": viewstategen,
        }

        dbf_bytes: bytes | None = None

        for tag in download_targets:
            onclick = tag.get("onclick", "")
            href    = tag.get("href", "")
            m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", onclick)
            if m:
                payload["__EVENTTARGET"]   = m.group(1)
                payload["__EVENTARGUMENT"] = m.group(2)
                r2 = retry(lambda: session.post(APPRAISER_URL, data=payload, timeout=60))
                ctype = r2.headers.get("content-type", "")
                if "zip" in ctype or "octet" in ctype or len(r2.content) > 50_000:
                    dbf_bytes = r2.content
                    log.info("Got bulk file via doPostBack (%d bytes)", len(dbf_bytes))
                    break
            elif href and href.lower().endswith((".zip", ".dbf")):
                url = href if href.startswith("http") else APPRAISER_URL.rsplit("/", 1)[0] + "/" + href
                r2 = retry(lambda: session.get(url, timeout=60))
                dbf_bytes = r2.content
                log.info("Got bulk file via direct link (%d bytes)", len(dbf_bytes))
                break

        if dbf_bytes is None:
            log.warning("Could not locate bulk parcel download — owner lookups will be empty")
            return owner_index

        # ── Step 3: parse DBF (possibly inside a ZIP) ────────────────────
        if not HAS_DBF:
            log.warning("dbfread not installed — skipping parcel parse")
            return owner_index

        # try ZIP first
        raw_dbf: bytes | None = None
        try:
            with zipfile.ZipFile(io.BytesIO(dbf_bytes)) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".dbf"):
                        raw_dbf = zf.read(name)
                        log.info("Extracted %s from ZIP", name)
                        break
        except zipfile.BadZipFile:
            raw_dbf = dbf_bytes  # assume it's a bare DBF

        if raw_dbf is None:
            log.warning("No DBF found inside downloaded file")
            return owner_index

        tmp = Path("/tmp/parcels.dbf")
        tmp.write_bytes(raw_dbf)

        table = DBF(str(tmp), load=True, ignore_missing_memofile=True)
        fields = [f.name.upper() for f in table.fields]
        log.info("DBF fields: %s", fields)

        def col(row: dict, *candidates: str) -> str:
            for c in candidates:
                v = row.get(c) or row.get(c.lower())
                if v:
                    return clean(v)
            return ""

        for row in table:
            row = {k.upper(): v for k, v in row.items()}
            owner = col(row, "OWNER", "OWN1", "OWN_NAME")
            if not owner:
                continue
            addr_info = {
                "prop_address": col(row, "SITE_ADDR", "SITEADDR", "SITE_ADDRESS"),
                "prop_city":    col(row, "SITE_CITY", "SITECITY"),
                "prop_state":   col(row, "SITE_STATE") or "NM",
                "prop_zip":     col(row, "SITE_ZIP", "SITEZIP"),
                "mail_address": col(row, "ADDR_1", "MAILADR1", "MAIL_ADDR"),
                "mail_city":    col(row, "CITY", "MAILCITY", "MAIL_CITY"),
                "mail_state":   col(row, "STATE", "MAIL_STATE") or "NM",
                "mail_zip":     col(row, "ZIP", "MAILZIP", "MAIL_ZIP"),
            }
            for variant in name_variants(owner):
                owner_index[variant] = addr_info

        log.info("Parcel index built: %d owner entries", len(owner_index))

    except Exception:
        log.warning("Parcel download failed:\n%s", traceback.format_exc())

    return owner_index


def lookup_owner(name: str, index: dict[str, dict]) -> dict:
    """Return address fields for an owner name, or empty dict."""
    for variant in name_variants(name):
        if variant in index:
            return index[variant]
    return {}


# ═══════════════════════════════════════════════════════════════════════════
# CLERK PORTAL  (Playwright)
# ═══════════════════════════════════════════════════════════════════════════

async def handle_disclaimer(page: Page) -> None:
    """Click through any disclaimer / terms page before the search form."""
    disclaimer_texts = [
        "Accept", "I Agree", "I Accept", "Agree", "Continue",
        "OK", "Yes", "Proceed", "Enter", "I understand",
    ]
    for txt in disclaimer_texts:
        try:
            btn = page.get_by_role("button", name=re.compile(txt, re.I))
            if await btn.count() > 0:
                log.info("  Clicking disclaimer button: '%s'", txt)
                await btn.click()
                await page.wait_for_load_state("networkidle", timeout=15_000)
                return
        except Exception:
            pass
    for txt in disclaimer_texts:
        try:
            link = page.get_by_role("link", name=re.compile(txt, re.I))
            if await link.count() > 0:
                log.info("  Clicking disclaimer link: '%s'", txt)
                await link.click()
                await page.wait_for_load_state("networkidle", timeout=15_000)
                return
        except Exception:
            pass


async def debug_page(page: Page, label: str) -> None:
    """Log page title, URL, visible text, and save a screenshot."""
    try:
        title = await page.title()
        url   = page.url
        text  = await page.inner_text("body")
        log.info("  [DEBUG %s] title=%r  url=%s", label, title, url)
        log.info("  [DEBUG %s] body=%s", label, text[:600].replace("\n", " "))
        shot_path = f"/tmp/debug_{label}.png"
        await page.screenshot(path=shot_path, full_page=False)
        img_b64 = base64.b64encode(Path(shot_path).read_bytes()).decode()
        log.info("  [SCREENSHOT_B64_START_%s]%s[SCREENSHOT_B64_END_%s]",
                 label, img_b64, label)
    except Exception as exc:
        log.warning("  [DEBUG %s] Could not capture: %s", label, exc)


async def clerk_search_type(
    page: Page,
    doc_type: str,
    date_from: str,
    date_to: str,
) -> list[dict]:
    """
    Search the Tyler clerk portal for one document type, return raw rows.
    date_from / date_to: "MM/DD/YYYY"
    """
    results: list[dict] = []
    is_first = (doc_type == TARGET_TYPES[0])

    try:
        log.info("  Searching clerk for type=%s", doc_type)

        # ── Navigate ─────────────────────────────────────────────────────
        await async_retry(
            lambda: page.goto(CLERK_SEARCH, wait_until="networkidle", timeout=20_000)
        )

        if is_first:
            await debug_page(page, "after_load")

        # ── Handle disclaimer ────────────────────────────────────────────
        await handle_disclaimer(page)

        if is_first:
            await debug_page(page, "after_disclaimer")

        # ── Wait for form ────────────────────────────────────────────────
        form_selectors = [
            "select", "input[type='text']", "input[type='search']",
            "#SearchFormControl", "form", ".search-form",
            "[id*='Search']", "[id*='search']",
        ]
        form_found = False
        for sel in form_selectors:
            try:
                await page.wait_for_selector(sel, timeout=5_000)
                form_found = True
                log.info("  Form found via: %s", sel)
                break
            except Exception:
                pass

        if not form_found:
            log.warning("  No search form found for %s", doc_type)
            if is_first:
                await debug_page(page, "no_form")
            return results

        # ── Select doc type ──────────────────────────────────────────────
        select_sels = [
            "select[name*='DocType']", "select[name*='doctype']",
            "select[id*='DocType']",   "select[id*='doctype']",
            "#cboDocType", "select[name='cboDocType']",
            "select[id*='Type']", "select",
        ]
        selected_doc = False
        for sel in select_sels:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    options = await el.evaluate(
                        "el => Array.from(el.options).map(o => o.value)"
                    )
                    if is_first:
                        log.info("  Select %s options sample: %s", sel, options[:15])
                    if doc_type in options:
                        await el.select_option(value=doc_type)
                        selected_doc = True
                        log.info("  Selected %s via %s", doc_type, sel)
                        break
            except Exception:
                pass

        if not selected_doc:
            try:
                await page.get_by_label(re.compile("doc.*type", re.I)).select_option(value=doc_type)
                selected_doc = True
            except Exception:
                pass

        if not selected_doc:
            log.warning("  Could not select doc type %s", doc_type)

        # ── Fill dates ───────────────────────────────────────────────────
        date_from_sels = [
            "input[name*='DateFrom']", "input[id*='DateFrom']",
            "input[name*='StartDate']","input[id*='StartDate']",
            "#txtDateFrom", "input[name='txtDateFrom']",
            "input[placeholder*='from']", "input[placeholder*='From']",
        ]
        date_to_sels = [
            "input[name*='DateTo']",   "input[id*='DateTo']",
            "input[name*='EndDate']",  "input[id*='EndDate']",
            "#txtDateTo", "input[name='txtDateTo']",
            "input[placeholder*='to']","input[placeholder*='To']",
        ]

        async def fill_first(sels: list[str], value: str) -> bool:
            for sel in sels:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.triple_click()
                        await el.fill(value)
                        return True
                except Exception:
                    pass
            return False

        await fill_first(date_from_sels, date_from)
        await fill_first(date_to_sels,   date_to)

        # ── Submit ───────────────────────────────────────────────────────
        submit_sels = [
            "input[type='submit']", "button[type='submit']",
            "input[value*='Search']","button:has-text('Search')",
            "#btnSearch", "input[name='btnSearch']",
            "button:has-text('Find')", "input[value*='Find']",
        ]
        submitted = False
        for sel in submit_sels:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click()
                    submitted = True
                    break
            except Exception:
                pass
        if not submitted:
            await page.keyboard.press("Enter")

        await page.wait_for_load_state("networkidle", timeout=30_000)

        if is_first:
            await debug_page(page, "after_search")

        # ── Paginate ─────────────────────────────────────────────────────
        page_num = 0
        while True:
            page_num += 1
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            rows = await parse_clerk_table(soup, doc_type, page.url)
            results.extend(rows)
            log.info("    Page %d — %d rows (total %d)", page_num, len(rows), len(results))

            next_btn = None
            for candidate in soup.find_all(["a", "input", "button"]):
                txt = candidate.get_text(strip=True).lower()
                val = candidate.get("value", "").lower()
                if txt in ("next", "next >", ">", ">>") or val in ("next", "next >", ">"):
                    next_btn = candidate
                    break
            if next_btn is None:
                break

            try:
                onclick = next_btn.get("onclick", "")
                m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", onclick)
                if m:
                    await page.evaluate(f"__doPostBack('{m.group(1)}', '{m.group(2)}')")
                else:
                    txt = next_btn.get_text(strip=True)
                    await page.get_by_text(re.compile(re.escape(txt), re.I)).first.click()
                await page.wait_for_load_state("networkidle", timeout=20_000)
            except Exception as exc:
                log.warning("    Could not advance page: %s", exc)
                break

            if page_num > 50:
                break

    except Exception:
        log.warning("Clerk search failed for %s:\n%s", doc_type, traceback.format_exc())

    return results


async def parse_clerk_table(soup: BeautifulSoup, doc_type: str, current_url: str) -> list[dict]:
    """
    Extract document rows from a Tyler host results page.
    Returns a list of raw dicts.
    """
    rows_out: list[dict] = []

    # Tyler portals typically render results in a <table> or a series of divs
    tables = soup.find_all("table")
    for table in tables:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if not headers:
            # check first row for headers
            first_row = table.find("tr")
            if first_row:
                headers = [td.get_text(strip=True).lower() for td in first_row.find_all("td")]

        # Only process tables that look like document result tables
        relevant_headers = {"doc", "document", "type", "grantor", "grantee", "date", "filed", "book", "page"}
        if not any(any(rh in h for rh in relevant_headers) for h in headers):
            continue

        col_map = {h: i for i, h in enumerate(headers)}

        def gcol(*names: str) -> int | None:
            for n in names:
                for h, i in col_map.items():
                    if n in h:
                        return i
            return None

        idx_docnum   = gcol("doc number", "doc num", "document number", "doc #", "inst")
        idx_doctype  = gcol("doc type", "type", "document type")
        idx_grantor  = gcol("grantor", "owner", "seller")
        idx_grantee  = gcol("grantee", "buyer")
        idx_date     = gcol("date filed", "filed", "record date", "date")
        idx_legal    = gcol("legal", "description")
        idx_amount   = gcol("amount", "consideration", "value")
        idx_book     = gcol("book", "bk")
        idx_pg       = gcol("page", "pg")

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if not cells:
                continue

            def cell(idx: int | None) -> str:
                if idx is None or idx >= len(cells):
                    return ""
                return clean(cells[idx].get_text(strip=True))

            doc_num  = cell(idx_docnum)
            filed    = cell(idx_date)
            grantor  = cell(idx_grantor)
            grantee  = cell(idx_grantee)
            legal    = cell(idx_legal)
            amount_r = cell(idx_amount)
            row_type = cell(idx_doctype) or doc_type

            if not doc_num and not filed:
                continue

            # build direct URL from link in the row, or construct one
            link_tag = tr.find("a", href=True)
            if link_tag:
                href = link_tag["href"]
                if href.startswith("http"):
                    clerk_url = href
                else:
                    clerk_url = CLERK_BASE + "/" + href.lstrip("/")
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

    # ── fallback: div-based results ──────────────────────────────────────
    if not rows_out:
        for div in soup.find_all("div", class_=re.compile(r"result|row|record", re.I)):
            text = div.get_text(" ", strip=True)
            doc_m = re.search(r"(?:Doc(?:ument)?\s*(?:Number|#|No)[:\s#]+)(\S+)", text, re.I)
            date_m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", text)
            if doc_m or date_m:
                link_tag = div.find("a", href=True)
                clerk_url = CLERK_BASE + "/" + link_tag["href"].lstrip("/") if link_tag else current_url
                rows_out.append({
                    "doc_num":   doc_m.group(1) if doc_m else "",
                    "doc_type":  doc_type,
                    "filed":     normalise_date(date_m.group(1) if date_m else ""),
                    "owner":     "",
                    "grantee":   "",
                    "amount":    None,
                    "legal":     "",
                    "clerk_url": clerk_url,
                })

    return rows_out


def normalise_date(raw: str) -> str:
    """Convert MM/DD/YYYY or YYYY-MM-DD → YYYY-MM-DD."""
    if not raw:
        return ""
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw  # return as-is if unparseable


# ═══════════════════════════════════════════════════════════════════════════
# MAIN SCRAPE ORCHESTRATION
# ═══════════════════════════════════════════════════════════════════════════

async def run_clerk_scrape(date_from: str, date_to: str) -> list[dict]:
    """Run Playwright scrape for all target doc types. Returns raw records."""
    all_rows: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
        )
        ctx: BrowserContext = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page: Page = await ctx.new_page()

        for doc_type in TARGET_TYPES:
            try:
                rows = await async_retry(
                    lambda dt=doc_type: clerk_search_type(page, dt, date_from, date_to),
                    attempts=1,
                    delay=2.0,
                )
                all_rows.extend(rows)
            except Exception:
                log.error("Failed all retries for doc_type=%s", doc_type)

        await browser.close()

    log.info("Clerk scrape complete — %d raw records across all types", len(all_rows))
    return all_rows


def build_records(
    raw: list[dict],
    parcel_index: dict[str, dict],
    date_from_iso: str,
    date_to_iso: str,
) -> dict:
    """
    Merge raw clerk rows with parcel data, compute scores, produce final output.
    """
    cutoff = datetime.fromisoformat(date_from_iso).date()
    to_dt  = datetime.fromisoformat(date_to_iso).date()

    records: list[dict] = []
    seen: set[str] = set()

    for raw_r in raw:
        try:
            doc_num  = clean(raw_r.get("doc_num", ""))
            doc_type = clean(raw_r.get("doc_type", "")).upper()
            filed    = clean(raw_r.get("filed", ""))
            owner    = clean(raw_r.get("owner", ""))
            grantee  = clean(raw_r.get("grantee", ""))
            amount   = raw_r.get("amount")
            legal    = clean(raw_r.get("legal", ""))
            clerk_url = clean(raw_r.get("clerk_url", ""))

            # dedup by doc_num + doc_type
            key = f"{doc_num}::{doc_type}"
            if key in seen:
                continue
            seen.add(key)

            # date filter
            if filed:
                try:
                    filed_dt = datetime.strptime(filed[:10], "%Y-%m-%d").date()
                    if not (cutoff <= filed_dt <= to_dt):
                        continue
                except ValueError:
                    pass

            cat, cat_label = DOC_TYPE_MAP.get(doc_type, ("other", doc_type))

            # parcel lookup
            parcel = lookup_owner(owner, parcel_index) if owner else {}

            rec: dict = {
                "doc_num":    doc_num,
                "doc_type":   doc_type,
                "filed":      filed,
                "cat":        cat,
                "cat_label":  cat_label,
                "owner":      owner,
                "grantee":    grantee,
                "amount":     amount,
                "legal":      legal,
                "prop_address": parcel.get("prop_address", ""),
                "prop_city":    parcel.get("prop_city", ""),
                "prop_state":   parcel.get("prop_state", "NM"),
                "prop_zip":     parcel.get("prop_zip", ""),
                "mail_address": parcel.get("mail_address", ""),
                "mail_city":    parcel.get("mail_city", ""),
                "mail_state":   parcel.get("mail_state", "NM"),
                "mail_zip":     parcel.get("mail_zip", ""),
                "clerk_url":  clerk_url,
                "flags":      [],
                "score":      0,
            }

            score, flags = score_record(rec)
            rec["score"] = score
            rec["flags"] = flags

            records.append(rec)

        except Exception:
            log.warning("Skipping bad record: %s\n%s", raw_r, traceback.format_exc())

    # sort by score desc, then date desc
    records.sort(key=lambda r: (-r["score"], r.get("filed", "") or ""), reverse=False)
    records.sort(key=lambda r: -r["score"])

    with_addr = sum(1 for r in records if r.get("prop_address"))

    output = {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "source":       "Bernalillo County Clerk / Assessor",
        "date_range":   f"{date_from_iso} to {date_to_iso}",
        "total":        len(records),
        "with_address": with_addr,
        "records":      records,
    }

    return output


# ═══════════════════════════════════════════════════════════════════════════
# GHL CSV EXPORT
# ═══════════════════════════════════════════════════════════════════════════

def export_ghl_csv(records: list[dict], path: Path) -> None:
    """Write a GoHighLevel-compatible CSV export."""
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for r in records:
            owner = r.get("owner", "")
            # split owner into first/last (best-effort)
            parts = owner.replace(",", " ").split()
            first = parts[-1] if len(parts) > 1 else ""
            last  = parts[0]  if parts else owner

            writer.writerow({
                "First Name":              first,
                "Last Name":               last,
                "Mailing Address":         r.get("mail_address", ""),
                "Mailing City":            r.get("mail_city", ""),
                "Mailing State":           r.get("mail_state", ""),
                "Mailing Zip":             r.get("mail_zip", ""),
                "Property Address":        r.get("prop_address", ""),
                "Property City":           r.get("prop_city", ""),
                "Property State":          r.get("prop_state", ""),
                "Property Zip":            r.get("prop_zip", ""),
                "Lead Type":               r.get("cat_label", ""),
                "Document Type":           r.get("doc_type", ""),
                "Date Filed":              r.get("filed", ""),
                "Document Number":         r.get("doc_num", ""),
                "Amount/Debt Owed":        r.get("amount", ""),
                "Seller Score":            r.get("score", ""),
                "Motivated Seller Flags":  "; ".join(r.get("flags", [])),
                "Source":                  "Bernalillo County Clerk",
                "Public Records URL":      r.get("clerk_url", ""),
            })
    log.info("GHL CSV written → %s (%d rows)", path, len(records))


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

async def main() -> None:
    log.info("=" * 60)
    log.info("Bernalillo County Motivated Seller Scraper")
    log.info("Lookback: %d days", LOOKBACK_DAYS)

    now      = datetime.now(timezone.utc)
    date_to  = now.date()
    date_from = (now - timedelta(days=LOOKBACK_DAYS)).date()

    date_from_iso = date_from.isoformat()          # YYYY-MM-DD
    date_to_iso   = date_to.isoformat()

    # Tyler portal uses MM/DD/YYYY
    date_from_str = date_from.strftime("%m/%d/%Y")
    date_to_str   = date_to.strftime("%m/%d/%Y")

    log.info("Date range: %s → %s", date_from_iso, date_to_iso)

    # ── 1. Parcel bulk download ──────────────────────────────────────────
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    })
    parcel_index = download_parcel_dbf(session)

    # ── 2. Clerk portal scrape ───────────────────────────────────────────
    raw_records = await run_clerk_scrape(date_from_str, date_to_str)

    # ── 3. Build final output ────────────────────────────────────────────
    output = build_records(raw_records, parcel_index, date_from_iso, date_to_iso)

    log.info("Final record count: %d  (with address: %d)",
             output["total"], output["with_address"])

    # ── 4. Write outputs ─────────────────────────────────────────────────
    for dest_path in [
        BASE_DIR / "dashboard" / "records.json",
        BASE_DIR / "data"      / "records.json",
    ]:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")
        log.info("Written → %s", dest_path)

    # ── 5. GHL CSV export ────────────────────────────────────────────────
    today_str = now.strftime("%Y%m%d")
    ghl_path  = BASE_DIR / "data" / f"ghl_export_{today_str}.csv"
    export_ghl_csv(output["records"], ghl_path)

    log.info("=" * 60)
    log.info("Scrape complete ✓")


if __name__ == "__main__":
    asyncio.run(main())
