"""
Bernalillo County, NM — Motivated Seller Lead Scraper  v6
==========================================================
CLERK  : Tyler Technologies Kiosk (no disclaimer)
         https://bernalillocountynm-kiosk.tylerhost.net/kiosk/search/DOCSEARCH4993S1
         • Playwright fills date-range form → intercepts the JSON API response
         • Falls back to HTML table parsing if JSON not captured

ADDRESS: City of Albuquerque / BernCo ArcGIS REST service (free, no auth)
         https://coageo.cabq.gov/cabqgeo/rest/services/agis/AddressReport/MapServer/4
         • Query by owner name → returns parcel + mailing address in JSON
         • Cache results so each owner is looked up only once
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import re
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page

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
BASE_DIR      = Path(__file__).resolve().parent.parent

# ── Clerk kiosk (no disclaimer / no login) ─────────────────────────────────
KIOSK_URL = (
    "https://bernalillocountynm-kiosk.tylerhost.net"
    "/kiosk/search/DOCSEARCH4993S1"
)

# ── ArcGIS parcel REST service (City of Albuquerque / BernCo Assessor) ──────
# MaxRecordCount: 20 000  |  Supports LIKE queries  |  No auth required
ARCGIS_PARCELS = (
    "https://coageo.cabq.gov/cabqgeo/rest/services"
    "/agis/AddressReport/MapServer/4/query"
)

# Fields we want from the parcel layer
ARCGIS_FIELDS = (
    "OWNER,UPC,"
    "SITUSADD,SITUSCITY,SITUSSTATE,SITUSZIP,"   # pre-built site address
    "OWNADD,OWNADD2,OWNCITY,OWNSTATE,OWNZIPCODE"  # pre-built mailing address
)

# ── Document type map ────────────────────────────────────────────────────────
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

DOC_KEYWORDS: dict[str, list[str]] = {
    "LP":       ["lis pendens"],
    "NOFC":     ["notice of foreclosure", "foreclosure"],
    "TAXDEED":  ["tax deed"],
    "JUD":      ["judgment", "judgement"],
    "CCJ":      ["certified judgment"],
    "DRJUD":    ["domestic"],
    "LNCORPTX": ["corp tax", "corporate tax"],
    "LNIRS":    ["irs"],
    "LNFED":    ["federal lien", "federal tax"],
    "LN":       ["lien"],
    "LNMECH":   ["mechanic"],
    "LNHOA":    ["hoa", "homeowner"],
    "MEDLN":    ["medicaid"],
    "PRO":      ["probate"],
    "NOC":      ["notice of commencement", "commencement"],
    "RELLP":    ["release lis pendens", "release lp"],
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def clean(s: Any) -> str:
    return " ".join(str(s).split()).strip() if s else ""


def parse_amount(raw: str) -> float | None:
    c = re.sub(r"[^\d.]", "", (raw or "").replace(",", ""))
    try:
        v = float(c)
        return v if v > 0 else None
    except ValueError:
        return None


def normalise_date(raw: str) -> str:
    if not raw:
        return ""
    raw = clean(raw)
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw


def name_variants(full: str) -> list[str]:
    full = full.upper().strip()
    variants: set[str] = {full}
    stripped = re.sub(
        r"\b(LLC|INC|CORP|TRUST|LTD|LLP|ETUX|ETAL)\b", "", full
    ).strip(" ,")
    variants.add(stripped)
    if "," in full:
        parts = [p.strip() for p in full.split(",", 1)]
        variants.update([
            f"{parts[0]} {parts[1]}",
            f"{parts[1]} {parts[0]}",
        ])
    else:
        words = full.split()
        if len(words) >= 2:
            variants.add(f"{words[-1]}, {' '.join(words[:-1])}")
            variants.add(f"{words[-1]} {' '.join(words[:-1])}")
    return [v for v in variants if v]


def guess_doc_type(raw: str) -> str:
    t = raw.upper().strip()
    if t in DOC_TYPE_MAP:
        return t
    tl = t.lower()
    for code, kws in DOC_KEYWORDS.items():
        if any(kw in tl for kw in kws):
            return code
    return t


def score_record(rec: dict) -> tuple[int, list[str]]:
    flags: list[str] = []
    score    = 30
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
    if re.search(r"\b(LLC|INC|CORP|LTD|LLP)\b", owner, re.I):
        flags.append("LLC / corp owner")
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).date()
        if filed and datetime.strptime(filed[:10], "%Y-%m-%d").date() >= cutoff:
            flags.append("New this week")
    except Exception:
        pass

    score += 10 * len(flags)
    if any(f in flags for f in ("Lis pendens", "Pre-foreclosure")):
        score += 20
    if amount:
        score += 15 if amount > 100_000 else (10 if amount > 50_000 else 0)
    if "New this week" in flags:
        score += 5
    if rec.get("prop_address"):
        score += 5

    return min(score, 100), flags


# ═══════════════════════════════════════════════════════════════════════════
# ARCGIS PARCEL LOOKUP  (free, no auth)
# ═══════════════════════════════════════════════════════════════════════════

def _arcgis_query(session: requests.Session, where: str) -> list[dict]:
    """Run one ArcGIS REST query and return feature attributes."""
    params = {
        "where":          where,
        "outFields":      ARCGIS_FIELDS,
        "returnGeometry": "false",
        "resultRecordCount": 5,   # first match is enough
        "f":              "json",
    }
    try:
        r = session.get(ARCGIS_PARCELS, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        return [feat["attributes"] for feat in data.get("features", [])]
    except Exception as exc:
        log.debug("ArcGIS query failed ('%s'): %s", where, exc)
        return []


def _build_address(attrs: dict) -> dict:
    """
    Build address dict from ArcGIS parcel attributes.
    Uses pre-built combined address fields SITUSADD and OWNADD.
    """
    def f(key: str) -> str:
        return clean(attrs.get(key, "") or "")

    return {
        "prop_address": f("SITUSADD"),
        "prop_city":    f("SITUSCITY") or "Albuquerque",
        "prop_state":   f("SITUSSTATE") or "NM",
        "prop_zip":     f("SITUSZIP"),
        "mail_address": f("OWNADD"),
        "mail_city":    f("OWNCITY"),
        "mail_state":   f("OWNSTATE") or "NM",
        "mail_zip":     f("OWNZIPCODE"),
    }


def lookup_address(
    session: requests.Session,
    owner: str,
    cache: dict[str, dict],
) -> dict:
    """
    Look up a property owner in the BernCo/CABQ ArcGIS parcel service.
    Returns address dict, or empty dict on no match.
    Uses an in-memory cache to avoid duplicate queries.
    """
    if not owner:
        return {}
    key = owner.upper().strip()
    if key in cache:
        return cache[key]

    # Build search variants — try last name first for best match rate
    owner_clean = re.sub(r"[^\w\s]", " ", owner).strip()
    words = owner_clean.upper().split()
    last_name = words[0] if words else owner_clean.upper()

    # Search strategies: last name only → full name → variants
    search_terms = [last_name]
    if len(words) > 1:
        search_terms.append(" ".join(words[:2]))   # first two words
    for v in name_variants(owner)[:2]:
        if v.upper() not in search_terms:
            search_terms.append(v.upper())

    for term in search_terms[:4]:
        safe = term.replace("'", "''")
        # Use standard SQL LIKE without UPPER() — service is case-insensitive
        where = f"OWNER LIKE '{safe}%'"
        features = _arcgis_query(session, where)
        if features:
            addr = _build_address(features[0])
            if addr.get("prop_address") or addr.get("mail_address"):
                log.info("    ArcGIS match for '%s' (term='%s'): %s",
                         owner, term, addr.get("prop_address", addr.get("mail_address","(no addr)")))
                cache[key] = addr
                return addr
        time.sleep(0.3)

    log.debug("    No ArcGIS match for '%s'", owner)
    cache[key] = {}
    return {}


# ═══════════════════════════════════════════════════════════════════════════
# PLAYWRIGHT — KIOSK SEARCH
# ═══════════════════════════════════════════════════════════════════════════

async def _type_into(page: Page, selector: str, value: str) -> bool:
    """
    Reliably fill a React-managed input by clicking it, selecting all,
    then typing the value character by character to trigger React's onChange.
    """
    try:
        loc = page.locator(selector).first
        if await loc.count() == 0:
            return False
        await loc.click()
        await page.keyboard.press("Control+a")
        await loc.type(value, delay=50)
        await page.keyboard.press("Tab")   # trigger onBlur / validate
        return True
    except Exception:
        return False


async def fill_date_field(page: Page, label_hint: str, value: str) -> bool:
    """Fill a date field using multiple selector strategies."""
    strategies = [
        f"input[placeholder*='{label_hint}']",
        f"input[aria-label*='{label_hint}']",
        f"input[id*='{label_hint.lower().replace(' ','')}']",
        f"input[name*='{label_hint.lower().replace(' ','')}']",
    ]
    # Also try label-based lookup
    for sel in strategies:
        if await _type_into(page, sel, value):
            log.info("    Filled '%s' via %s", label_hint, sel)
            return True
    # Label-based approach
    try:
        await page.get_by_label(re.compile(label_hint, re.I)).first.type(
            value, delay=50
        )
        await page.keyboard.press("Tab")
        log.info("    Filled '%s' via get_by_label", label_hint)
        return True
    except Exception:
        pass
    return False


async def _parse_html_results(page: Page) -> list[dict]:
    """
    Parse results using the confirmed ss-search-row class structure.

    Each result row is a <li class="ss-search-row"> with text:
      {letter}\n{doc_num} \xa0•\xa0 {date} {time} \xa0•\xa0 {doc_type}\nGrantor...\n{owner}
    and href pointing to the document detail page.
    """
    rows_out: list[dict] = []
    KIOSK_BASE = "https://bernalillocountynm-kiosk.tylerhost.net"

    try:
        # Target ONLY ss-search-row elements — confirmed class from DOM snapshot
        results = await page.evaluate("""
            () => [...document.querySelectorAll('li.ss-search-row')]
                .map(el => {
                    // Get all child li text for owner names
                    const childLis = [...el.querySelectorAll('li')];
                    const names = childLis.map(li => (li.innerText||'').trim()).filter(Boolean);

                    // Get the main row link
                    const link = el.querySelector('a[href]');
                    return {
                        text: (el.innerText || el.textContent || '').trim(),
                        href: link ? link.href : '',
                        names: names
                    };
                })
        """)

        log.info("  ss-search-row elements found: %d", len(results))

        # Log first 3 items so we can verify parsing
        for item in results[:3]:
            log.info("  ROW text=%s | href=%s",
                     repr(item["text"][:120]), item["href"][-60:])

        # Parse each result row
        sep = "\xa0\u2022\xa0"  # •  separator Tyler uses

        for item in results:
            text = item["text"]
            href = item["href"]
            if not text:
                continue

            # Split on the bullet separator (\xa0•\xa0)
            # Text looks like: "M\n2026028527 \xa0•\xa0 04/09/2026 04:53 PM \xa0•\xa0 Mortgage\nGrantor..."
            parts = re.split(r"\s*\xa0\u2022\xa0\s*|\s*\u00a0•\u00a0\s*|\xa0•\xa0", text)

            # Fallback: split on bullet character directly
            if len(parts) < 3:
                parts = re.split(r"\s*•\s*", text)

            # Extract doc number from first part (after stripping leading letter+newline)
            first = parts[0] if parts else text
            doc_m = re.search(r"(\d{7,})", first)
            doc_num = doc_m.group(1) if doc_m else ""

            # Extract date from second part
            date_part = parts[1] if len(parts) > 1 else ""
            date_m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", date_part)
            filed = normalise_date(date_m.group(1)) if date_m else ""

            # Extract doc type from third part (before any newline)
            type_raw = parts[2].split("\n")[0].strip() if len(parts) > 2 else ""

            # Extract owner: look in text after "Grantor" label
            owner = ""
            grantor_m = re.search(
                r"Grantor(?:\s*\(\d+\))?\s*\n([^\n]+)", text
            )
            if grantor_m:
                owner = grantor_m.group(1).strip()
            elif item.get("names"):
                owner = item["names"][0]

            # Extract legal from text after "Legal Summary"
            legal_m = re.search(r"Legal Summary(?:\s*\(\d+\))?\s*\n([^\n]+)", text)
            legal = legal_m.group(1).strip() if legal_m else ""

            if not doc_num and not filed:
                continue

            if href and not href.startswith("http"):
                href = KIOSK_BASE + ("" if href.startswith("/") else "/kiosk/") + href.lstrip("/")

            rows_out.append({
                "doc_num":   doc_num,
                "doc_type":  guess_doc_type(type_raw),
                "filed":     filed,
                "owner":     owner,
                "grantee":   "",
                "amount":    None,
                "legal":     legal,
                "clerk_url": href or page.url,
            })

        log.info("  Parsed %d rows from ss-search-row elements", len(rows_out))

    except Exception as exc:
        log.warning("  ss-search-row parse failed: %s\n%s", exc, traceback.format_exc())

    return rows_out


async def _extract_json_results(api_data: list) -> list[dict]:
    """
    Convert intercepted API JSON responses to our row format.
    Handles non-dict payloads (e.g. session pings that return -1 or True).
    Logs the full key structure of each payload so we can see the real shape.
    """
    rows: list[dict] = []

    for payload in api_data:
        # Skip non-dict, non-list payloads (session pings return -1, True, etc.)
        if not isinstance(payload, (dict, list)):
            log.info("  Skipping scalar API payload: %s", repr(payload)[:80])
            continue

        # Log keys for dicts so we can see the real structure
        if isinstance(payload, dict):
            log.info("  API payload keys: %s  | sample: %s",
                     list(payload.keys())[:20], str(payload)[:600])

        # Locate the list of result items — try every known key shape
        items = None
        if isinstance(payload, dict):
            for key in ("results", "documents", "rows", "records", "items",
                        "hits", "searchResults", "Rows", "Records", "Documents",
                        "data", "Results"):
                val = payload.get(key)
                if isinstance(val, list) and val:
                    items = val
                    log.info("  Found %d items under key '%s'", len(items), key)
                    break
                # "data" might itself be a dict containing a list
                if isinstance(val, dict):
                    for sub_key in ("results", "rows", "records", "documents", "items"):
                        sub = val.get(sub_key)
                        if isinstance(sub, list) and sub:
                            items = sub
                            log.info("  Found %d items under data.%s", len(items), sub_key)
                            break
                    if items:
                        break
        # Flat list at top level
        if items is None and isinstance(payload, list) and payload:
            items = payload
            log.info("  Payload is a flat list of %d items", len(items))

        if not items:
            log.info("  No result items found in this payload — skipping")
            continue

        for item in items:
            if not isinstance(item, dict):
                continue

            # Tyler uses various field names — try them all
            def f(*keys):
                for k in keys:
                    v = item.get(k) or item.get(k.lower()) or item.get(k.upper())
                    if v:
                        return clean(str(v))
                return ""

            doc_num   = f("instrumentNumber","docNumber","documentNumber","instNumber","number")
            doc_type  = f("documentType","docType","instrumentType","type","typeCode")
            filed_raw = f("recordedDate","recordingDate","filedDate","dateRecorded","date")
            grantor   = f("grantor","grantorName","owner","ownerName")
            grantee   = f("grantee","granteeName","buyer")
            legal     = f("legalDescription","legal","description")
            amount_r  = f("consideration","amount","value","salePrice")
            detail_url= f("detailUrl","url","link","documentUrl")

            if not doc_num and not filed_raw:
                continue

            if detail_url and not detail_url.startswith("http"):
                detail_url = (
                    "https://bernalillocountynm-kiosk.tylerhost.net"
                    + ("" if detail_url.startswith("/") else "/kiosk/")
                    + detail_url.lstrip("/")
                )

            rows.append({
                "doc_num":   doc_num,
                "doc_type":  guess_doc_type(doc_type) if doc_type else "",
                "filed":     normalise_date(filed_raw),
                "owner":     grantor,
                "grantee":   grantee,
                "amount":    parse_amount(amount_r),
                "legal":     legal,
                "clerk_url": detail_url,
            })

    return rows


async def run_kiosk_scrape(date_from: str, date_to: str) -> list[dict]:
    """
    Search the Tyler kiosk for ALL documents in the date range,
    then filter locally for our doc types.
    Uses API response interception for clean JSON data; falls back to HTML parsing.
    """
    all_rows: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"],
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        await ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        page = await ctx.new_page()

        # ── Set up API response interceptor ──────────────────────────────
        captured_api: list[dict] = []

        async def capture(response):
            url   = response.url.lower()
            ctype = response.headers.get("content-type", "")
            # Capture ALL JSON from the kiosk domain — we'll filter later
            if "json" in ctype and "tylerhost.net" in url:
                try:
                    data = await response.json()
                    captured_api.append(data)
                    log.info("  Captured JSON from: %s", response.url[-80:])
                except Exception:
                    pass

        page.on("response", capture)

        # ── Load kiosk — should show the search form immediately ──────────
        log.info("Loading kiosk URL…")
        await page.goto(KIOSK_URL, wait_until="networkidle", timeout=30_000)

        title = await page.title()
        url   = page.url
        log.info("  Loaded: '%s'  url=%s", title, url)

        if "disclaimer" in url.lower():
            log.warning("  ⚠ Kiosk redirected to a disclaimer — trying to accept it")
            try:
                btn = page.get_by_text("I Accept", exact=True)
                if await btn.count() > 0:
                    await btn.first.click()
                    await page.wait_for_load_state("networkidle", timeout=15_000)
                    log.info("  Disclaimer accepted → %s", page.url)
            except Exception as e:
                log.warning("  Could not accept disclaimer: %s", e)

        # Wait for the form to render
        try:
            await page.wait_for_selector(
                "input, [role='textbox'], [contenteditable]",
                timeout=15_000,
            )
        except Exception:
            log.warning("  Form inputs not found — page may still be loading")

        # Log all visible inputs for debugging
        try:
            inputs = await page.evaluate("""
                () => [...document.querySelectorAll('input,select,textarea')]
                    .map(el => ({
                        tag: el.tagName, type: el.type||'', name: el.name||'',
                        id: el.id||'', placeholder: el.placeholder||'',
                        ariaLabel: el.getAttribute('aria-label')||'',
                        value: el.value||''
                    }))
            """)
            log.info("  Form inputs found: %s", [
                {k:v for k,v in i.items() if v} for i in inputs[:12]
            ])
        except Exception:
            pass

        # ── Fill Recording Date Start & End ──────────────────────────────
        # We know the exact field name attributes from the live page:
        #   field_RecDateID_DOT_StartDate  and  field_RecDateID_DOT_EndDate
        # Use fill() which sets value AND fires synthetic input/change events.
        async def fill_known(field_name: str, value: str) -> bool:
            sel = f"input[name='{field_name}']"
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    await loc.click()
                    await loc.fill(value)
                    # Dispatch extra events so React/jQuery registers the change
                    await page.evaluate(f"""
                        () => {{
                            const el = document.querySelector("{sel}");
                            if (!el) return;
                            el.value = "{value}";
                            ['input','change','blur'].forEach(t =>
                                el.dispatchEvent(new Event(t, {{bubbles:true}}))
                            );
                        }}
                    """)
                    await page.keyboard.press("Tab")
                    val_after = await loc.input_value()
                    log.info("    Filled %s = '%s' (confirmed: '%s')",
                             field_name, value, val_after)
                    return True
            except Exception as exc:
                log.warning("    fill_known failed for %s: %s", field_name, exc)
            return False

        filled_from = await fill_known("field_RecDateID_DOT_StartDate", date_from)
        filled_to   = await fill_known("field_RecDateID_DOT_EndDate",   date_to)

        # Fallback to label-based approach if exact names change
        if not filled_from:
            filled_from = await fill_date_field(page, "Recording Date Start", date_from)
        if not filled_to:
            filled_to = await fill_date_field(page, "Recording Date End", date_to)

        log.info("  Dates filled: from=%s to=%s", filled_from, filled_to)

        # Screenshot for debugging (helps diagnose form state)
        try:
            shot = Path("/tmp/kiosk_form.png")
            await page.screenshot(path=str(shot))
            import base64
            b64 = base64.b64encode(shot.read_bytes()).decode()
            log.info("[SCREENSHOT_FORM_B64]%s[/SCREENSHOT_FORM_B64]", b64[:200] + "...")
        except Exception:
            pass

        # ── Click Search ──────────────────────────────────────────────────
        submitted = False
        for loc in [
            page.get_by_role("button", name=re.compile(r"^search$", re.I)),
            page.get_by_role("button", name=re.compile(r"search", re.I)),
            page.locator("button[type='submit']"),
            page.locator("input[type='submit']"),
            page.get_by_text("Search", exact=True),
            page.locator("[class*='search'][class*='button']"),
            page.locator("[class*='Search'][class*='Button']"),
        ]:
            try:
                if await loc.count() > 0:
                    await loc.first.click()
                    submitted = True
                    log.info("  Clicked Search button")
                    break
            except Exception:
                pass

        if not submitted:
            await page.keyboard.press("Enter")
            log.info("  Submitted via Enter")

        # Wait for results (either JSON API response or HTML table)
        await page.wait_for_load_state("networkidle", timeout=25_000)
        await asyncio.sleep(2)   # extra settle time for React rendering

        log.info("  After search: url=%s  captured_api=%d",
                 page.url, len(captured_api))

        # ── Extract totalPages from the pagination JSON ───────────────────
        total_pages = 1
        for payload in captured_api:
            if isinstance(payload, dict) and "totalPages" in payload:
                total_pages = int(payload.get("totalPages", 1))
                log.info("  totalPages=%d  currentPage=%d",
                         total_pages, payload.get("currentPage", 1))
                break
        captured_api.clear()

        # ── Wait for jQuery Mobile to render results into the DOM ─────────
        await asyncio.sleep(3)

        # ── Paginate: POST each page directly ────────────────────────────
        # The kiosk POSTs to searchPost/ with a page parameter.
        # We reconstruct those requests via Playwright evaluate.
        log.info("  Fetching %d pages of HTML results…", total_pages)

        for page_num in range(1, min(total_pages + 1, 51)):
            # For pages 2+, trigger the page change via the pager
            if page_num > 1:
                try:
                    # Click the Next button if enabled, otherwise use JS
                    html_check = await page.content()
                    soup_check = BeautifulSoup(html_check, "lxml")
                    next_disabled = False
                    for tag in soup_check.find_all(["a","button"]):
                        if tag.get_text(strip=True).lower() in ("next","next >",">",">>"):
                            if "disabled" in " ".join(tag.get("class",[])).lower():
                                next_disabled = True
                            break
                    if next_disabled:
                        log.info("  Next button disabled at page %d — done", page_num)
                        break

                    # Try clicking Next
                    nxt = page.locator("a[data-role='button']:not(.ui-disabled)")
                    nxt_texts = []
                    for i in range(await nxt.count()):
                        t = await nxt.nth(i).inner_text()
                        nxt_texts.append(t.strip().lower())
                    log.info("  Enabled buttons: %s", nxt_texts[:10])

                    clicked = False
                    for i in range(await nxt.count()):
                        t = (await nxt.nth(i).inner_text()).strip().lower()
                        if t in ("next", "next >", ">", ">>"):
                            await nxt.nth(i).click()
                            await page.wait_for_load_state("networkidle", timeout=15_000)
                            await asyncio.sleep(2)
                            clicked = True
                            break
                    if not clicked:
                        log.info("  Could not find enabled Next — done at page %d", page_num)
                        break
                except Exception as e:
                    log.warning("  Page navigation error: %s", e)
                    break

            batch = await _parse_html_results(page)
            log.info("  Page %d/%d: %d rows", page_num, total_pages, len(batch))
            all_rows.extend(batch)

            if page_num == 1 and len(batch) == 0:
                # Log raw HTML so we can see what the results look like
                html_sample = await page.content()
                log.info("  Page 1 HTML sample (2000 chars):\n%s",
                         html_sample[:2000])

        await browser.close()

    log.info("Kiosk scrape done — %d raw rows before filtering", len(all_rows))
    return all_rows


# ═══════════════════════════════════════════════════════════════════════════
# BUILD FINAL RECORDS
# ═══════════════════════════════════════════════════════════════════════════

def build_records(
    raw: list[dict],
    date_from_iso: str,
    date_to_iso: str,
) -> dict:
    cutoff = datetime.fromisoformat(date_from_iso).date()
    to_dt  = datetime.fromisoformat(date_to_iso).date()

    # Shared requests session for ArcGIS lookups
    sess = requests.Session()
    sess.headers.update(HEADERS)
    addr_cache: dict[str, dict] = {}

    records: list[dict] = []
    seen: set[str]      = set()

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

            # Dedup
            key = f"{doc_num}::{doc_type}"
            if key in seen:
                continue
            seen.add(key)

            # Date filter
            if filed:
                try:
                    fd = datetime.strptime(filed[:10], "%Y-%m-%d").date()
                    if not (cutoff <= fd <= to_dt):
                        continue
                except ValueError:
                    pass

            # Map doc type — Tyler uses full names like "Lis Pendens", "Lien", "Judgment"
            if doc_type in DOC_TYPE_MAP:
                cat, cat_label = DOC_TYPE_MAP[doc_type]
            else:
                tl = doc_type.lower()
                if "lis pendens" in tl:
                    cat, cat_label, doc_type = "foreclosure", "Lis Pendens", "LP"
                elif "foreclosure" in tl:
                    cat, cat_label, doc_type = "foreclosure", "Notice of Foreclosure", "NOFC"
                elif "tax deed" in tl:
                    cat, cat_label, doc_type = "tax", "Tax Deed", "TAXDEED"
                elif "judgment" in tl or "judgement" in tl:
                    cat, cat_label, doc_type = "judgment", "Judgment", "JUD"
                elif "mechanic" in tl:
                    cat, cat_label, doc_type = "lien", "Mechanic Lien", "LNMECH"
                elif "hoa" in tl or "homeowner" in tl:
                    cat, cat_label, doc_type = "lien", "HOA Lien", "LNHOA"
                elif "irs" in tl:
                    cat, cat_label, doc_type = "lien", "IRS Lien", "LNIRS"
                elif "federal" in tl and "lien" in tl:
                    cat, cat_label, doc_type = "lien", "Federal Lien", "LNFED"
                elif "corp" in tl and "tax" in tl:
                    cat, cat_label, doc_type = "lien", "Corp Tax Lien", "LNCORPTX"
                elif "lien" in tl:
                    cat, cat_label, doc_type = "lien", "Lien", "LN"
                elif "probate" in tl:
                    cat, cat_label, doc_type = "probate", "Probate", "PRO"
                elif "commencement" in tl:
                    cat, cat_label, doc_type = "other", "Notice of Commencement", "NOC"
                else:
                    # Skip document types we don't care about
                    continue

            # ── Address from ArcGIS parcel service ────────────────────────
            addr: dict = {}
            if owner:
                addr = lookup_address(sess, owner, addr_cache)

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
                "prop_address": addr.get("prop_address", ""),
                "prop_city":    addr.get("prop_city",    ""),
                "prop_state":   addr.get("prop_state",   "NM"),
                "prop_zip":     addr.get("prop_zip",     ""),
                "mail_address": addr.get("mail_address", ""),
                "mail_city":    addr.get("mail_city",    ""),
                "mail_state":   addr.get("mail_state",   "NM"),
                "mail_zip":     addr.get("mail_zip",     ""),
                "clerk_url":    clerk_url,
                "flags":        [],
                "score":        0,
            }
            score, flags = score_record(rec)
            rec["score"]  = score
            rec["flags"]  = flags
            records.append(rec)

        except Exception:
            log.warning("Skipping bad record:\n%s", traceback.format_exc())

    records.sort(key=lambda r: -r["score"])
    with_addr = sum(1 for r in records if r.get("prop_address"))
    log.info("Address hit rate: %d / %d records", with_addr, len(records))

    return {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "source":       "Bernalillo County Clerk — Tyler Kiosk",
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
    cols = [
        "First Name","Last Name",
        "Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number",
        "Amount/Debt Owed","Seller Score","Motivated Seller Flags",
        "Source","Public Records URL",
    ]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for r in records:
            parts = r.get("owner","").replace(","," ").split()
            w.writerow({
                "First Name":             parts[-1] if len(parts) > 1 else "",
                "Last Name":              parts[0]  if parts else "",
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
                "Source":                 "Bernalillo County Clerk",
                "Public Records URL":     r.get("clerk_url",   ""),
            })
    log.info("GHL CSV → %s (%d rows)", path, len(records))


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

async def main() -> None:
    log.info("=" * 60)
    log.info("Bernalillo County Motivated Seller Scraper  v6")
    log.info("Clerk  : %s", KIOSK_URL)
    log.info("Address: ArcGIS REST (coageo.cabq.gov)")
    log.info("Lookback: %d days", LOOKBACK_DAYS)

    now       = datetime.now(timezone.utc)
    date_to   = now.date()
    date_from = (now - timedelta(days=LOOKBACK_DAYS)).date()

    date_from_iso = date_from.isoformat()
    date_to_iso   = date_to.isoformat()
    date_from_str = date_from.strftime("%m/%d/%Y")
    date_to_str   = date_to.strftime("%m/%d/%Y")

    log.info("Date range: %s → %s", date_from_iso, date_to_iso)

    # 1. Scrape clerk records
    raw = await run_kiosk_scrape(date_from_str, date_to_str)

    # 2. Enrich + score
    output = build_records(raw, date_from_iso, date_to_iso)
    log.info("Final: %d records  (%d with address)",
             output["total"], output["with_address"])

    # 3. Write JSON
    for dest in [
        BASE_DIR / "dashboard" / "records.json",
        BASE_DIR / "data"      / "records.json",
    ]:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(
            json.dumps(output, indent=2, default=str), encoding="utf-8"
        )
        log.info("Written → %s", dest)

    # 4. GHL CSV
    export_ghl_csv(
        output["records"],
        BASE_DIR / "data" / f"ghl_export_{now.strftime('%Y%m%d')}.csv",
    )

    log.info("=" * 60)
    log.info("Scrape complete ✓")


if __name__ == "__main__":
    asyncio.run(main())
