"""
Bernalillo County, NM — Motivated Seller Lead Scraper  v4
==========================================================
Clerk records  : requests + BeautifulSoup → eagleweb.bernco.gov
Address lookup : per-owner search → assessor.bernco.gov (iasWorld)

No Playwright, no bulk DBF.  Plain HTTP throughout.

Outputs: dashboard/records.json  +  data/records.json
"""

from __future__ import annotations

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

# ── Eagle Web (Clerk) ──────────────────────────────────────────────────────
CLERK_BASE        = "https://eagleweb.bernco.gov"
CLERK_DISCLAIMER  = f"{CLERK_BASE}/recorder/web/loginPOST.jsp?guest=true"
CLERK_DOC_SEARCH  = f"{CLERK_BASE}/recorder/eagleweb/docSearch.jsp"
CLERK_DOC_RESULTS = f"{CLERK_BASE}/recorder/eagleweb/doDocSearch.jsp"

# ── Assessor (iasWorld) ────────────────────────────────────────────────────
ASSESSOR_BASE        = "https://assessor.bernco.gov"
ASSESSOR_DISCLAIMER  = (
    f"{ASSESSOR_BASE}/public.access/Search/Disclaimer.aspx"
    "?FromUrl=../search/commonsearch.aspx?mode=realprop"
)
ASSESSOR_SEARCH      = f"{ASSESSOR_BASE}/public.access/search/commonsearch.aspx?mode=realprop"
ASSESSOR_OWNER_RESULTS = f"{ASSESSOR_BASE}/public.access/search/ownersearchresults.aspx"

# Polite delay between assessor lookups (seconds)
ASSESSOR_DELAY = 1.0

# ── Document type mapping ──────────────────────────────────────────────────
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
    "LP":       ["lis pendens", "lispendens", " lp"],
    "NOFC":     ["notice of foreclosure", "foreclosure notice", "nofc"],
    "TAXDEED":  ["tax deed", "taxdeed"],
    "JUD":      ["judgment", "judgement", " jud"],
    "CCJ":      ["certified judgment", "ccj"],
    "DRJUD":    ["domestic", "drjud"],
    "LNCORPTX": ["corp tax", "corporate tax", "lncorptx"],
    "LNIRS":    ["irs lien", " irs ", "lnirs"],
    "LNFED":    ["federal lien", "federal tax lien", "lnfed"],
    "LN":       [" lien"],
    "LNMECH":   ["mechanic", "mechanics", "lnmech"],
    "LNHOA":    ["hoa", "homeowner", "lnhoa"],
    "MEDLN":    ["medicaid", "medln"],
    "PRO":      ["probate", " pro"],
    "NOC":      ["notice of commencement", "commencement", " noc"],
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
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# ═══════════════════════════════════════════════════════════════════════════
# GENERIC HELPERS
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


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def get_hidden_fields(soup: BeautifulSoup) -> dict:
    return {
        inp["name"]: inp.get("value", "")
        for inp in soup.find_all("input", {"type": "hidden"})
        if inp.get("name")
    }


# ═══════════════════════════════════════════════════════════════════════════
# SCORING
# ═══════════════════════════════════════════════════════════════════════════

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
# ASSESSOR — PER-OWNER ADDRESS LOOKUP  (iasWorld)
# ═══════════════════════════════════════════════════════════════════════════

def accept_assessor_disclaimer(session: requests.Session) -> bool:
    """
    GET the assessor disclaimer, then POST 'Agree'.
    iasWorld sets a session cookie after this.
    """
    log.info("Accepting assessor disclaimer…")
    try:
        r = retry(lambda: session.get(ASSESSOR_DISCLAIMER, timeout=20))
        soup = BeautifulSoup(r.text, "lxml")
        hidden = get_hidden_fields(soup)

        # iasWorld disclaimer buttons — try common names
        for btn_name, btn_val in [
            ("btAgree",  "Agree"),
            ("btnAgree", "Agree"),
            ("btAccept", "Accept"),
            ("agree",    "Agree"),
            ("accept",   "Accept"),
            ("submit",   "Agree"),
        ]:
            payload = {**hidden, btn_name: btn_val}
            r2 = retry(lambda p=payload: session.post(
                ASSESSOR_DISCLAIMER, data=p,
                timeout=20, allow_redirects=True
            ))
            if "disclaimer" not in r2.url.lower():
                log.info("  Assessor disclaimer accepted → %s", r2.url)
                return True

        # Last resort: try the search page directly
        r3 = retry(lambda: session.get(ASSESSOR_SEARCH, timeout=20))
        if r3.status_code == 200 and "search" in r3.text.lower():
            log.info("  Assessor accessible without disclaimer POST")
            return True

        log.warning("  Could not accept assessor disclaimer")
        return False
    except Exception:
        log.warning("Assessor disclaimer error:\n%s", traceback.format_exc())
        return False


def _parse_iasworld_address(soup: BeautifulSoup) -> dict:
    """
    Parse property + mailing address from an iasWorld property detail page.
    Returns a dict with prop_address, prop_city, prop_state, prop_zip,
    mail_address, mail_city, mail_state, mail_zip.
    """
    out = {
        "prop_address": "", "prop_city": "", "prop_state": "NM", "prop_zip": "",
        "mail_address": "", "mail_city": "", "mail_state": "NM", "mail_zip": "",
    }

    text = soup.get_text(" ", strip=True)

    # ── Try structured table cells first ────────────────────────────────
    # iasWorld detail pages use label/value table rows
    label_map: dict[str, str] = {}
    for row in soup.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) >= 2:
            lbl = clean(cells[0].get_text()).lower()
            val = clean(cells[1].get_text())
            if lbl and val:
                label_map[lbl] = val

    # Property (site) address
    for lbl in ("site address", "property address", "location address",
                "situs address", "physical address", "site addr"):
        if lbl in label_map:
            out["prop_address"] = label_map[lbl]
            break

    for lbl in ("site city", "property city", "city"):
        if lbl in label_map and not out["prop_city"]:
            out["prop_city"] = label_map[lbl]

    for lbl in ("site zip", "property zip", "zip", "zip code"):
        if lbl in label_map and not out["prop_zip"]:
            out["prop_zip"] = label_map[lbl]

    # Mailing address
    for lbl in ("mailing address", "mail address", "mail addr",
                "owner address", "taxpayer address"):
        if lbl in label_map:
            out["mail_address"] = label_map[lbl]
            break

    for lbl in ("mailing city", "mail city"):
        if lbl in label_map and not out["mail_city"]:
            out["mail_city"] = label_map[lbl]

    for lbl in ("mailing state", "mail state"):
        if lbl in label_map and not out["mail_state"]:
            out["mail_state"] = label_map[lbl]

    for lbl in ("mailing zip", "mail zip"):
        if lbl in label_map and not out["mail_zip"]:
            out["mail_zip"] = label_map[lbl]

    # ── Regex fallbacks on raw text ──────────────────────────────────────
    if not out["prop_address"]:
        # Common NM address pattern: 1234 SOME ST NW, ALBUQUERQUE NM 87102
        m = re.search(
            r"\b(\d{1,5}\s+[A-Z0-9 ]+(?:ST|AVE|BLVD|RD|DR|LN|CT|WAY|PL|CIR)"
            r"(?:\s+[NSEW]{1,2})?)\b",
            text.upper()
        )
        if m:
            out["prop_address"] = m.group(1).strip()

    if not out["prop_zip"]:
        m = re.search(r"\b(87\d{3})\b", text)
        if m:
            out["prop_zip"] = m.group(1)

    return out


def lookup_address_from_assessor(
    session: requests.Session,
    owner_name: str,
    cache: dict[str, dict],
) -> dict:
    """
    Search the Bernalillo County Assessor (iasWorld) by owner name.
    Returns address fields dict, or empty dict on failure.
    Uses an in-memory cache to avoid duplicate lookups.
    """
    if not owner_name:
        return {}

    owner_key = owner_name.upper().strip()
    if owner_key in cache:
        return cache[owner_key]

    # Be polite — rate limit
    time.sleep(ASSESSOR_DELAY)

    try:
        # ── Step 1: GET search page for hidden fields ────────────────────
        r = retry(lambda: session.get(ASSESSOR_SEARCH, timeout=20))
        soup = BeautifulSoup(r.text, "lxml")

        # If we hit a disclaimer, accept it and retry
        if "disclaimer" in r.url.lower() or "agree" in r.text.lower()[:2000]:
            accept_assessor_disclaimer(session)
            r = retry(lambda: session.get(ASSESSOR_SEARCH, timeout=20))
            soup = BeautifulSoup(r.text, "lxml")

        hidden = get_hidden_fields(soup)

        # Log available search fields once
        inputs = [(i.get("name",""), i.get("type",""), i.get("value","")[:20])
                  for i in soup.find_all("input") if i.get("name")]
        log.debug("  Assessor form inputs: %s", inputs[:10])

        # ── Step 2: POST owner name search ───────────────────────────────
        # iasWorld owner search field is usually "ownerName" or "owner"
        search_payloads = [
            {**hidden, "ownerName": owner_name, "searchType": "Owner",
             "submitButton": "Search"},
            {**hidden, "ownerName": owner_name, "Submit": "Search"},
            {**hidden, "owner":     owner_name, "searchType": "Owner",
             "submitButton": "Search"},
            {**hidden, "txtOwner":  owner_name, "btnSearch": "Search"},
            {**hidden, "OwnerName": owner_name, "searchType": "2",
             "Submit":       "Search"},
        ]

        results_soup: BeautifulSoup | None = None
        for payload in search_payloads:
            r2 = retry(lambda p=payload: session.post(
                ASSESSOR_SEARCH, data=p,
                timeout=20, allow_redirects=True
            ))
            s2 = BeautifulSoup(r2.text, "lxml")
            # Check if we got actual results (a table with property rows)
            tables = s2.find_all("table")
            has_results = any(
                len(t.find_all("tr")) > 1 for t in tables
            )
            text_lower = r2.text.lower()
            if has_results and any(
                kw in text_lower
                for kw in ("parcel", "owner", "address", "account", "situs")
            ):
                results_soup = s2
                log.info("  Assessor results found for '%s'", owner_name)
                break

        if results_soup is None:
            log.debug("  No assessor results for '%s'", owner_name)
            cache[owner_key] = {}
            return {}

        # ── Step 3: Parse the results list ───────────────────────────────
        # iasWorld shows a table of matching properties; pick the first one
        detail_url: str | None = None
        for a in results_soup.find_all("a", href=True):
            href = a["href"]
            if any(kw in href.lower() for kw in
                   ("parceldetail", "detail", "account", "parcel")):
                if href.startswith("http"):
                    detail_url = href
                elif href.startswith("/"):
                    detail_url = ASSESSOR_BASE + href
                else:
                    detail_url = (
                        ASSESSOR_BASE
                        + "/public.access/search/"
                        + href.lstrip("/")
                    )
                break

        # If no detail link, try parsing address directly from results page
        if detail_url is None:
            addr = _parse_iasworld_address(results_soup)
            cache[owner_key] = addr
            return addr

        # ── Step 4: GET the property detail page ─────────────────────────
        time.sleep(ASSESSOR_DELAY)
        r3 = retry(lambda u=detail_url: session.get(u, timeout=20))
        detail_soup = BeautifulSoup(r3.text, "lxml")
        addr = _parse_iasworld_address(detail_soup)
        log.info("  Address for '%s': %s", owner_name, addr.get("prop_address","(none)"))
        cache[owner_key] = addr
        return addr

    except Exception:
        log.warning("Assessor lookup failed for '%s':\n%s",
                    owner_name, traceback.format_exc())
        cache[owner_key] = {}
        return {}


# ═══════════════════════════════════════════════════════════════════════════
# EAGLE WEB CLERK  (requests + BeautifulSoup)
# ═══════════════════════════════════════════════════════════════════════════

def accept_clerk_disclaimer(session: requests.Session) -> bool:
    log.info("Accepting Eagle Web disclaimer…")
    try:
        r = retry(lambda: session.get(CLERK_DISCLAIMER, timeout=20))
        soup = BeautifulSoup(r.text, "lxml")
        hidden = get_hidden_fields(soup)

        log.info("Disclaimer buttons: %s", [
            (t.name, t.get("type",""), t.get("value","")[:30], t.get_text(strip=True)[:30])
            for t in soup.find_all(["input","button","a"])
            if t.get("type") != "hidden"
        ][:15])

        # Try various POST payloads
        for payload in [
            {**hidden, "disclaimer": "true"},
            {**hidden, "action": "accept"},
            {**hidden, "accept": "I Accept"},
            {**hidden, "AcceptDisclaimer": "true"},
            hidden,
        ]:
            r2 = retry(lambda p=payload: session.post(
                CLERK_DISCLAIMER, data=p, timeout=20, allow_redirects=True
            ))
            if "disclaimer" not in r2.url.lower() and len(r2.text) > 500:
                log.info("Clerk disclaimer accepted → %s", r2.url)
                return True

        # Try the doc search page directly
        r3 = retry(lambda: session.get(CLERK_DOC_SEARCH, timeout=20))
        if r3.status_code == 200 and "search" in r3.text.lower():
            log.info("Clerk doc search accessible directly")
            return True

        log.warning("Could not definitively clear clerk disclaimer — proceeding anyway")
        return False
    except Exception:
        log.warning("Clerk disclaimer error:\n%s", traceback.format_exc())
        return False


def discover_doc_types(session: requests.Session) -> dict[str, str]:
    log.info("Discovering Eagle Web doc type codes…")
    option_map: dict[str, str] = {}
    try:
        r = retry(lambda: session.get(CLERK_DOC_SEARCH, timeout=20))
        soup = BeautifulSoup(r.text, "lxml")

        for sel in soup.find_all("select"):
            sel_name = sel.get("name", "").lower()
            opts = [(o.get("value",""), o.get_text(strip=True))
                    for o in sel.find_all("option")]
            if opts:
                log.info("  Select '%s' — %d options: %s",
                         sel_name, len(opts), opts[:6])

            if any(kw in sel_name for kw in
                   ("doctype","type","instrument","rectype","doctypes")):
                for val, text in opts:
                    if not val:
                        continue
                    text_l = " " + text.lower() + " "
                    for our_code, keywords in DOC_KEYWORDS.items():
                        if any(kw in text_l for kw in keywords):
                            if our_code not in option_map:
                                option_map[our_code] = val
                                log.info("    Mapped %s → '%s' ('%s')",
                                         our_code, val, text)
                            break
                    val_up = val.upper().strip()
                    if val_up in DOC_TYPE_MAP and val_up not in option_map:
                        option_map[val_up] = val

    except Exception:
        log.warning("Could not discover doc types:\n%s", traceback.format_exc())

    for code in DOC_TYPE_MAP:
        if code not in option_map:
            option_map[code] = code

    log.info("Doc type map: %d entries", len(option_map))
    return option_map


def search_doc_type(
    session: requests.Session,
    doc_type_code: str,
    eagle_value: str,
    date_from: str,
    date_to: str,
    form_hidden: dict,
) -> list[dict]:
    results: list[dict] = []

    post_bodies = [
        {**form_hidden, "searchType": "Document",
         "recordedDateFrom": date_from, "recordedDateTo": date_to,
         "doctypes": eagle_value, "submitButton": "Search"},
        {**form_hidden, "searchType": "Document",
         "dateFrom": date_from, "dateTo": date_to,
         "docType": eagle_value, "Submit": "Search"},
        {**form_hidden, "searchType": "Document",
         "recordedDateFrom": date_from, "recordedDateTo": date_to,
         "recType": eagle_value, "submitButton": "Search"},
    ]

    for attempt, body in enumerate(post_bodies):
        try:
            log.info("  Searching %s (pattern %d)…", doc_type_code, attempt + 1)
            r = retry(lambda b=body: session.post(
                CLERK_DOC_RESULTS, data=b, timeout=30, allow_redirects=True
            ))
            soup = BeautifulSoup(r.text, "lxml")
            title = soup.title.string if soup.title else "?"
            log.info("    URL: %s  title: %s", r.url, title)

            rows = parse_results_page(soup, doc_type_code, r.url)
            all_rows = list(rows)

            # Paginate
            page_num = 1
            while True:
                next_url = find_next_page(soup, r.url)
                if not next_url or page_num >= 50:
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
                log.info("    Found %d records", len(all_rows))
                results = all_rows
                break

        except Exception:
            log.warning("    Pattern %d failed:\n%s",
                        attempt + 1, traceback.format_exc())

    log.info("  %s → %d records", doc_type_code, len(results))
    return results


def parse_results_page(
    soup: BeautifulSoup, doc_type: str, current_url: str
) -> list[dict]:
    rows_out: list[dict] = []

    for table in soup.find_all("table"):
        headers_raw = [th.get_text(strip=True).lower()
                       for th in table.find_all("th")]
        if not headers_raw:
            first_tr = table.find("tr")
            if first_tr:
                headers_raw = [td.get_text(strip=True).lower()
                               for td in first_tr.find_all("td")]

        relevant = {"instrument","doc","type","grantor","grantee",
                    "recorded","date","book","page","legal"}
        if not any(any(rk in h for rk in relevant) for h in headers_raw):
            continue

        col = {h: i for i, h in enumerate(headers_raw)}

        def gcol(*names: str) -> int | None:
            for n in names:
                for h, i in col.items():
                    if n in h:
                        return i
            return None

        idx_num     = gcol("instrument","doc number","doc num",
                           "document number","doc #","number","inst #")
        idx_type    = gcol("doc type","type","instrument type","rectype")
        idx_grantor = gcol("grantor","owner","seller")
        idx_grantee = gcol("grantee","buyer")
        idx_date    = gcol("recorded date","date recorded","recorded",
                           "date filed","filed date","date")
        idx_legal   = gcol("legal","description","legal desc")
        idx_amount  = gcol("amount","consideration","value")

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

    return rows_out


def find_next_page(soup: BeautifulSoup, current_url: str) -> str | None:
    for tag in soup.find_all(["a", "input", "button"]):
        txt = (tag.get_text(strip=True) + tag.get("value","")).lower().strip()
        if txt in ("next", "next >", ">", ">>", "next page"):
            href = tag.get("href","")
            if href:
                if href.startswith("http"):
                    return href
                if href.startswith("/"):
                    return CLERK_BASE + href
                return CLERK_BASE + "/recorder/eagleweb/" + href.lstrip("/")
            onclick = tag.get("onclick","")
            m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", onclick)
            if m:
                return (current_url
                        + f"?__EVENTTARGET={m.group(1)}"
                        + f"&__EVENTARGUMENT={m.group(2)}")
    return None


def run_clerk_scrape(
    session: requests.Session, date_from: str, date_to: str
) -> list[dict]:
    """Scrape Eagle Web for all target doc types."""
    all_rows: list[dict] = []

    accept_clerk_disclaimer(session)
    doc_type_options = discover_doc_types(session)

    # Get hidden form fields
    try:
        r = session.get(CLERK_DOC_SEARCH, timeout=20)
        form_hidden = get_hidden_fields(BeautifulSoup(r.text, "lxml"))
    except Exception:
        form_hidden = {}

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
            log.error("Failed searching %s:\n%s",
                      our_code, traceback.format_exc())

    log.info("Clerk scrape complete — %d raw records", len(all_rows))
    return all_rows


# ═══════════════════════════════════════════════════════════════════════════
# BUILD FINAL RECORDS
# ═══════════════════════════════════════════════════════════════════════════

def build_records(
    raw: list[dict],
    session: requests.Session,
    date_from_iso: str,
    date_to_iso: str,
) -> dict:
    """
    Merge raw clerk rows with per-owner assessor lookups,
    compute scores, deduplicate, and return final output dict.
    """
    cutoff = datetime.fromisoformat(date_from_iso).date()
    to_dt  = datetime.fromisoformat(date_to_iso).date()

    # Accept assessor disclaimer once for the whole session
    accept_assessor_disclaimer(session)
    address_cache: dict[str, dict] = {}

    records: list[dict] = []
    seen: set[str] = set()
    total_raw = len(raw)

    for idx, raw_r in enumerate(raw):
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

            # Date filter
            if filed:
                try:
                    filed_dt = datetime.strptime(filed[:10], "%Y-%m-%d").date()
                    if not (cutoff <= filed_dt <= to_dt):
                        continue
                except ValueError:
                    pass

            cat, cat_label = DOC_TYPE_MAP.get(doc_type, ("other", doc_type))

            # ── Address lookup via assessor ──────────────────────────────
            addr: dict = {}
            if owner:
                log.info("  [%d/%d] Looking up address for: %s",
                         idx + 1, total_raw, owner)
                # Try primary name then common variants
                for variant in name_variants(owner)[:3]:
                    addr = lookup_address_from_assessor(
                        session, variant, address_cache
                    )
                    if addr.get("prop_address"):
                        break

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
            rec["score"] = score
            rec["flags"] = flags
            records.append(rec)

        except Exception:
            log.warning("Skipping bad record: %s\n%s",
                        raw_r, traceback.format_exc())

    records.sort(key=lambda r: -r["score"])
    with_addr = sum(1 for r in records if r.get("prop_address"))

    log.info("Address hit rate: %d / %d records have a property address",
             with_addr, len(records))

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
    log.info("GHL CSV → %s  (%d rows)", path, len(records))


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    log.info("=" * 60)
    log.info("Bernalillo County Motivated Seller Scraper  v4")
    log.info("Clerk  : %s", CLERK_BASE)
    log.info("Assessor: %s", ASSESSOR_BASE)
    log.info("Lookback: %d days", LOOKBACK_DAYS)

    now       = datetime.now(timezone.utc)
    date_to   = now.date()
    date_from = (now - timedelta(days=LOOKBACK_DAYS)).date()

    date_from_iso = date_from.isoformat()
    date_to_iso   = date_to.isoformat()
    date_from_str = date_from.strftime("%m/%d/%Y")
    date_to_str   = date_to.strftime("%m/%d/%Y")

    log.info("Date range: %s → %s", date_from_iso, date_to_iso)

    # One shared session for all HTTP (clerk + assessor)
    session = make_session()

    # 1. Scrape clerk records
    raw_records = run_clerk_scrape(session, date_from_str, date_to_str)

    # 2. Enrich with assessor addresses + build output
    output = build_records(raw_records, session, date_from_iso, date_to_iso)
    log.info("Final: %d records  (%d with address)",
             output["total"], output["with_address"])

    # 3. Write JSON outputs
    for dest_path in [
        BASE_DIR / "dashboard" / "records.json",
        BASE_DIR / "data"      / "records.json",
    ]:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_text(
            json.dumps(output, indent=2, default=str), encoding="utf-8"
        )
        log.info("Written → %s", dest_path)

    # 4. GHL CSV
    today_str = now.strftime("%Y%m%d")
    export_ghl_csv(
        output["records"],
        BASE_DIR / "data" / f"ghl_export_{today_str}.csv"
    )

    log.info("=" * 60)
    log.info("Scrape complete ✓")


if __name__ == "__main__":
    main()
