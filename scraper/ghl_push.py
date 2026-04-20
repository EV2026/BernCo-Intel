"""
BernCo Intel — GoHighLevel Contact + Opportunity Push
=======================================================
Two modes:
  1. Default (no args):   reads dashboard/records.json and pushes hot leads
  2. CSV mode (--csv):    reads a BatchLeads skip-traced CSV and pushes all rows

In both modes every contact is also created as an Opportunity
in the Seller Lead Pipeline automatically.

Usage:
  python ghl_push.py                          # records.json mode
  python ghl_push.py --csv path/to/file.csv   # BatchLeads CSV mode
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── Config ────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent.parent
RECORDS_PATH  = BASE_DIR / "dashboard" / "records.json"

GHL_API_KEY     = os.environ["GHL_API_KEY"]
GHL_LOCATION_ID = os.environ["GHL_LOCATION_ID"]
HOT_THRESHOLD   = int(os.environ.get("HOT_THRESHOLD", "50"))

# Opportunity settings — must match JustJarvis exactly
PIPELINE_NAME = os.environ.get("GHL_PIPELINE_NAME", "Seller Lead Pipeline")
STAGE_NAME    = os.environ.get("GHL_STAGE_NAME",    "New Lead")

GHL_BASE    = "https://services.leadconnectorhq.com"
GHL_HEADERS = {
    "Authorization": f"Bearer {GHL_API_KEY}",
    "Content-Type":  "application/json",
    "Version":       "2021-07-28",
}

# Resolved at startup
PIPELINE_ID = None
STAGE_ID    = None

# ── Tag colours by category ───────────────────────────────────────────────
CATEGORY_TAGS = {
    "foreclosure": "Lis Pendens",
    "judgment":    "Judgment Lien",
    "lien":        "Lien",
    "tax":         "Tax Lien",
    "probate":     "Probate",
    "release":     "Release",
    "other":       "Other",
}


# ══════════════════════════════════════════════════════════════════════════
# API Helpers
# ══════════════════════════════════════════════════════════════════════════

def ghl_request(method: str, endpoint: str, payload: dict = None,
                retries: int = 3) -> dict | None:
    """Make a GHL API request with retries."""
    url = f"{GHL_BASE}{endpoint}"
    for attempt in range(retries):
        try:
            resp = requests.request(
                method, url,
                headers=GHL_HEADERS,
                json=payload,
                timeout=20,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                print(f"  Rate limited — waiting {wait}s…")
                time.sleep(wait)
                continue
            if resp.status_code in (200, 201):
                return resp.json()
            print(f"  GHL {method} {endpoint} → {resp.status_code}: {resp.text[:200]}")
            return None
        except Exception as exc:
            print(f"  Request error (attempt {attempt+1}): {exc}")
            time.sleep(3)
    return None


def resolve_pipeline() -> bool:
    """
    Fetch all pipelines and find the matching pipeline + stage IDs by name.
    Stores results in globals PIPELINE_ID and STAGE_ID.
    Returns True on success.
    """
    global PIPELINE_ID, STAGE_ID

    result = ghl_request("GET", f"/opportunities/pipelines?locationId={GHL_LOCATION_ID}")
    if not result:
        print(f"  ✗ Could not fetch pipelines from GHL")
        return False

    pipelines = result.get("pipelines", [])
    for pipeline in pipelines:
        if pipeline.get("name", "").strip().lower() == PIPELINE_NAME.strip().lower():
            PIPELINE_ID = pipeline["id"]
            for stage in pipeline.get("stages", []):
                if stage.get("name", "").strip().lower() == STAGE_NAME.strip().lower():
                    STAGE_ID = stage["id"]
                    break
            break

    if not PIPELINE_ID:
        available = [p.get("name") for p in pipelines]
        print(f"  ✗ Pipeline '{PIPELINE_NAME}' not found. Available: {available}")
        return False

    if not STAGE_ID:
        matched_pipeline = next(p for p in pipelines if p["id"] == PIPELINE_ID)
        available_stages = [s.get("name") for s in matched_pipeline.get("stages", [])]
        print(f"  ✗ Stage '{STAGE_NAME}' not found in '{PIPELINE_NAME}'. Available: {available_stages}")
        return False

    print(f"  ✓ Pipeline: '{PIPELINE_NAME}' ({PIPELINE_ID})")
    print(f"  ✓ Stage:    '{STAGE_NAME}' ({STAGE_ID})")
    return True


# ══════════════════════════════════════════════════════════════════════════
# Name / Entity Helpers
# ══════════════════════════════════════════════════════════════════════════

def parse_name(owner: str) -> tuple[str, str]:
    """Parse LAST FIRST kiosk format → (first, last)."""
    owner = owner.replace(",", " ").strip()
    parts = owner.split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return "", parts[0].title()
    last  = parts[0].title()
    first_candidates = parts[1:]
    if len(first_candidates) > 1 and len(first_candidates[-1]) == 1:
        first_candidates = first_candidates[:-1]
    first = first_candidates[0].title() if first_candidates else ""
    return first, last


def is_entity(owner: str) -> bool:
    """Return True if owner looks like an LLC/corp."""
    entity_re = re.compile(
        r"\b(LLC|INC|CORP|LLP|LP|LTD|TRUST|PC|PA|PLLC|NA|BANK|"
        r"CREDIT\s+UNION|FEDERAL|ASSOCIATION|COUNTY|CITY|STATE|"
        r"DEPARTMENT|HOSPITAL|CENTER|FOUNDATION|SERVICES|SOLUTIONS|"
        r"GROUP|PROPERTIES|HOLDINGS|MANAGEMENT|VENTURES|ENTERPRISES|"
        r"INVESTMENTS?)\b",
        re.I
    )
    return bool(entity_re.search(owner))


# ══════════════════════════════════════════════════════════════════════════
# records.json Mode — Payload Builders
# ══════════════════════════════════════════════════════════════════════════

def build_contact_payload(rec: dict) -> dict:
    """Build the GHL contact creation payload from a records.json lead record."""
    owner  = rec.get("owner", "")
    first, last = parse_name(owner)

    tags = ["BernCo Intel", "Motivated Seller"]
    cat_tag = CATEGORY_TAGS.get(rec.get("cat", "other"), "Other")
    tags.append(cat_tag)
    if rec.get("doc_type"):
        tags.append(rec["doc_type"])
    tags.extend(rec.get("flags", []))
    if is_entity(owner):
        tags.append("LLC / Corp Owner")

    payload = {
        "locationId": GHL_LOCATION_ID,
        "firstName":  first,
        "lastName":   last,
        "name":       f"{first} {last}".strip() or owner,
        "address1":   rec.get("mail_address", ""),
        "city":       rec.get("mail_city", ""),
        "state":      rec.get("mail_state", "NM"),
        "postalCode": rec.get("mail_zip", ""),
        "country":    "US",
        "source":     "BernCo Intel",
        "tags":       tags,
    }
    payload = {k: v for k, v in payload.items() if v}
    payload["locationId"] = GHL_LOCATION_ID
    payload["tags"]       = tags
    return payload


def build_note(rec: dict) -> str:
    """Build a detailed note to attach to the GHL contact."""
    flags  = "; ".join(rec.get("flags", [])) or "None"
    prop   = rec.get("prop_address", "N/A")
    if rec.get("prop_city"):
        prop += f", {rec['prop_city']}"
    amount = f"${rec['amount']:,.0f}" if rec.get("amount") else "N/A"

    return (
        f"🏠 BernCo Motivated Seller Lead\n"
        f"{'='*40}\n"
        f"Document #:    {rec.get('doc_num', 'N/A')}\n"
        f"Type:          {rec.get('cat_label', rec.get('doc_type', 'N/A'))}\n"
        f"Date Filed:    {rec.get('filed', 'N/A')}\n"
        f"Score:         {rec.get('score', 0)}/100\n"
        f"Flags:         {flags}\n"
        f"Amount:        {amount}\n"
        f"\nProperty Address:\n  {prop}\n"
        f"\nPublic Record:\n  {rec.get('clerk_url', 'N/A')}\n"
        f"\nSource: Bernalillo County Clerk — Tyler Kiosk\n"
        f"Imported: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )


def contact_exists(doc_num: str) -> str | None:
    """
    Check if a contact with this doc number tag already exists.
    Returns contact ID if found, None otherwise.
    """
    ghl_request(
        "GET",
        f"/contacts/search/duplicate?locationId={GHL_LOCATION_ID}&name={doc_num}",
    )
    search = ghl_request(
        "POST",
        "/contacts/search",
        {
            "locationId": GHL_LOCATION_ID,
            "filters": [{"field": "tags", "operator": "contains",
                         "value": doc_num}],
            "pageLimit": 1,
        }
    )
    if search and search.get("contacts"):
        return search["contacts"][0].get("id")
    return None


def opportunity_exists(contact_id: str) -> bool:
    """
    Check if an opportunity already exists for this contact in our pipeline.
    Returns True if found, False otherwise.
    """
    if not PIPELINE_ID:
        return False
    result = ghl_request(
        "GET",
        f"/opportunities/search?locationId={GHL_LOCATION_ID}&contactId={contact_id}&pipelineId={PIPELINE_ID}",
    )
    if result and result.get("opportunities"):
        return True
    return False


# ══════════════════════════════════════════════════════════════════════════
# Opportunity Creation (shared by both modes)
# ══════════════════════════════════════════════════════════════════════════

def create_opportunity(contact_id: str, opp_name: str, lead_type: str = "",
                       score: int = 0) -> bool:
    """
    Create an opportunity in the Seller Lead Pipeline linked to a contact.
    Returns True on success.
    """
    if not PIPELINE_ID or not STAGE_ID:
        print("  ⚠ Pipeline/stage not resolved — skipping opportunity creation")
        return False

    payload = {
        "locationId":    GHL_LOCATION_ID,
        "pipelineId":    PIPELINE_ID,
        "pipelineStageId": STAGE_ID,
        "contactId":     contact_id,
        "name":          opp_name,
        "status":        "open",
    }

    result = ghl_request("POST", "/opportunities/", payload)
    if result and (result.get("opportunity") or result.get("id")):
        opp_id = (result.get("opportunity") or {}).get("id") or result.get("id")
        print(f"  ✓ Opportunity created: '{opp_name}' ({opp_id})")
        return True

    print(f"  ✗ Failed to create opportunity for: {opp_name}")
    return False


# ══════════════════════════════════════════════════════════════════════════
# records.json Mode
# ══════════════════════════════════════════════════════════════════════════

def push_contact(rec: dict) -> bool:
    """Push one lead from records.json to GHL as contact + opportunity."""
    doc_num = rec.get("doc_num", "")
    owner   = rec.get("owner", "Unknown")

    existing_id = contact_exists(doc_num)
    if existing_id:
        print(f"  ↷ Already in GHL: {owner} ({doc_num})")
        # Still create opportunity if one doesn't exist yet
        if PIPELINE_ID and not opportunity_exists(existing_id):
            prop = rec.get("prop_address", "").strip()
            if rec.get("prop_city"):
                prop += f", {rec['prop_city']}"
            opp_name = prop or owner
            create_opportunity(
                contact_id=existing_id,
                opp_name=opp_name,
                lead_type=rec.get("cat_label", rec.get("doc_type", "")),
                score=rec.get("score", 0),
            )
        return True

    payload = build_contact_payload(rec)
    if doc_num:
        payload["tags"].append(doc_num)

    print(f"  → Sending: {owner}")
    result = ghl_request("POST", "/contacts/", payload)
    if not result:
        print(f"  ✗ Failed to create contact: {owner}")
        return False

    contact_id = (result.get("contact") or {}).get("id") or result.get("id")
    if not contact_id:
        print(f"  ✗ No contact ID in response for: {owner}")
        return False

    # Attach note
    note_body = build_note(rec)
    ghl_request("POST", f"/contacts/{contact_id}/notes", {
        "body": note_body,
        "userId": "",
    })

    # Build opportunity name from property address
    prop = rec.get("prop_address", "").strip()
    if rec.get("prop_city"):
        prop += f", {rec['prop_city']}"
    opp_name = prop or owner

    create_opportunity(
        contact_id=contact_id,
        opp_name=opp_name,
        lead_type=rec.get("cat_label", rec.get("doc_type", "")),
        score=rec.get("score", 0),
    )

    print(f"  ✓ Contact created: {owner} ({doc_num}) — score {rec.get('score', 0)}")
    time.sleep(0.3)
    return True


# ══════════════════════════════════════════════════════════════════════════
# BatchLeads CSV Mode
# ══════════════════════════════════════════════════════════════════════════

def extract_phones_from_row(row: dict) -> list[str]:
    """
    Pull all non-empty phone numbers from BatchLeads skip trace columns.
    Returns a list of up to 5 numbers, highest-scored first (column order = score order).
    """
    phones = []
    for i in range(5):
        num = row.get(f"Skiptrace:phoneNumbers.{i}.number", "").strip()
        if num:
            phones.append(num)
    return phones


def extract_emails_from_row(row: dict) -> list[str]:
    """Pull all non-empty emails from BatchLeads skip trace columns."""
    emails = []
    for i in range(3):
        email = row.get(f"Skiptrace:emails.{i}.email", "").strip()
        if email:
            emails.append(email)
    return emails


def build_contact_payload_from_csv(row: dict) -> dict:
    """
    Build a GHL contact payload from a BatchLeads CSV row.
    Prefers skip trace name/address data when available.
    """
    # Prefer skip traced name; fall back to original columns
    first = (row.get("Skiptrace:name.first") or row.get("First Name") or "").strip().title()
    last  = (row.get("Skiptrace:name.last")  or row.get("Last Name")  or "").strip().title()

    # Prefer skip traced mailing address
    mail_addr = (row.get("Skiptrace:mailingAddress.street") or row.get("Mailing Address") or "").strip()
    mail_city = (row.get("Skiptrace:mailingAddress.city")   or row.get("Mailing City")    or "").strip()
    mail_state= (row.get("Skiptrace:mailingAddress.state")  or row.get("Mailing State")   or "NM").strip()
    mail_zip  = (row.get("Skiptrace:mailingAddress.zip")    or row.get("Mailing Zip")     or "").strip()

    lead_type = row.get("Lead Type", "").strip()
    doc_num   = row.get("Document Number", "").strip()
    score     = row.get("Seller Score", "0").strip() or "0"
    flags     = row.get("Motivated Seller Flags", "").strip()

    # Build tags
    tags = ["BernCo Intel", "Motivated Seller", "Skip Traced"]
    if lead_type:
        tags.append(lead_type)
    if flags:
        tags.extend([f.strip() for f in flags.split(";") if f.strip()])
    if doc_num:
        tags.append(doc_num)
    owner_name = f"{first} {last}".strip()
    if is_entity(owner_name):
        tags.append("LLC / Corp Owner")

    payload = {
        "locationId": GHL_LOCATION_ID,
        "firstName":  first,
        "lastName":   last,
        "name":       owner_name,
        "address1":   mail_addr,
        "city":       mail_city,
        "state":      mail_state,
        "postalCode": mail_zip,
        "country":    "US",
        "source":     "BernCo Intel — BatchLeads",
        "tags":       tags,
    }

    # Add phones (GHL supports phone, phone2 ... for extras)
    phones = extract_phones_from_row(row)
    if phones:
        payload["phone"] = phones[0]
    # Additional phones go as customField or just logged in note — GHL v2 only
    # supports one native phone field, so extras are captured in the note below.

    # Add primary email
    emails = extract_emails_from_row(row)
    if emails:
        payload["email"] = emails[0]

    # Remove empty strings but keep required fields
    payload = {k: v for k, v in payload.items() if v}
    payload["locationId"] = GHL_LOCATION_ID
    payload["tags"]       = tags
    return payload, phones, emails


def build_note_from_csv(row: dict, phones: list, emails: list) -> str:
    """Build a note for a BatchLeads CSV contact."""
    prop_addr  = row.get("Property Address", "N/A").strip()
    prop_city  = row.get("Property City", "").strip()
    if prop_city:
        prop_addr += f", {prop_city}"
    prop_state = row.get("Property State", "").strip()
    prop_zip   = row.get("Property Zip", "").strip()
    if prop_zip:
        prop_addr += f" {prop_zip}"

    doc_num   = row.get("Document Number", "N/A").strip()
    lead_type = row.get("Lead Type", "N/A").strip()
    date_filed= row.get("Date Filed", "N/A").strip()
    score     = row.get("Seller Score", "N/A").strip()
    flags     = row.get("Motivated Seller Flags", "None").strip()
    clerk_url = row.get("Public Records URL", "N/A").strip()
    amount    = row.get("Amount/Debt Owed", "").strip()

    extra_phones = "\n    ".join(phones[1:]) if len(phones) > 1 else "None"
    extra_emails = "\n    ".join(emails[1:]) if len(emails) > 1 else "None"

    return (
        f"🏠 BernCo Motivated Seller Lead (Skip Traced)\n"
        f"{'='*40}\n"
        f"Document #:     {doc_num}\n"
        f"Type:           {lead_type}\n"
        f"Date Filed:     {date_filed}\n"
        f"Score:          {score}/100\n"
        f"Flags:          {flags}\n"
        f"Amount:         {amount or 'N/A'}\n"
        f"\nProperty Address:\n  {prop_addr}\n"
        f"\nAdditional Phones:\n    {extra_phones}\n"
        f"\nAdditional Emails:\n    {extra_emails}\n"
        f"\nPublic Record:\n  {clerk_url}\n"
        f"\nSource: Bernalillo County Clerk — Tyler Kiosk / BatchLeads\n"
        f"Imported: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )


def push_from_csv(csv_path: str) -> None:
    """
    Read a BatchLeads skip-traced CSV and push each row to GHL
    as a contact + opportunity.
    """
    print(f"\nCSV mode — reading: {csv_path}")
    path = Path(csv_path)
    if not path.exists():
        print(f"  ✗ File not found: {csv_path}")
        sys.exit(1)

    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"Total rows in CSV: {len(rows)}")

    pushed = skipped = failed = 0

    for row in rows:
        doc_num    = row.get("Document Number", "").strip()
        first_raw  = row.get("First Name", "").strip()
        last_raw   = row.get("Last Name", "").strip()
        label      = f"{first_raw} {last_raw}".strip() or doc_num or "Unknown"

        # Dedup check
        if doc_num:
            existing_id = contact_exists(doc_num)
            if existing_id:
                print(f"  ↷ Already in GHL: {label} ({doc_num})")
                # Still create opportunity if one doesn't exist yet
                if PIPELINE_ID and not opportunity_exists(existing_id):
                    prop = row.get("Property Address", "").strip()
                    prop_city = row.get("Property City", "").strip()
                    if prop_city:
                        prop += f", {prop_city}"
                    opp_name = prop or label
                    create_opportunity(
                        contact_id=existing_id,
                        opp_name=opp_name,
                        lead_type=row.get("Lead Type", ""),
                        score=int(row.get("Seller Score", "0") or 0),
                    )
                skipped += 1
                continue

        try:
            payload, phones, emails = build_contact_payload_from_csv(row)
        except Exception as e:
            print(f"  ✗ Payload build error for {label}: {e}")
            failed += 1
            continue

        print(f"  → Sending: {label}")
        result = ghl_request("POST", "/contacts/", payload)
        if not result:
            print(f"  ✗ Failed to create contact: {label}")
            failed += 1
            continue

        contact_id = (result.get("contact") or {}).get("id") or result.get("id")
        if not contact_id:
            print(f"  ✗ No contact ID returned for: {label}")
            failed += 1
            continue

        # Attach note with full lead details + extra phones/emails
        note_body = build_note_from_csv(row, phones, emails)
        ghl_request("POST", f"/contacts/{contact_id}/notes", {
            "body": note_body,
            "userId": "",
        })

        # Build opportunity name from property address
        prop = row.get("Property Address", "").strip()
        prop_city = row.get("Property City", "").strip()
        if prop_city:
            prop += f", {prop_city}"
        opp_name = prop or label

        create_opportunity(
            contact_id=contact_id,
            opp_name=opp_name,
            lead_type=row.get("Lead Type", ""),
            score=int(row.get("Seller Score", "0") or 0),
        )

        print(f"  ✓ Created: {label} ({doc_num})")
        pushed += 1
        time.sleep(0.3)

    print("=" * 50)
    print(f"Pushed: {pushed}  |  Skipped: {skipped}  |  Failed: {failed}  |  Total: {len(rows)}")
    print("Done ✓")


# ══════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="BernCo Intel → GoHighLevel Push")
    parser.add_argument("--csv", metavar="FILE",
                        help="Path to a BatchLeads skip-traced CSV file")
    args = parser.parse_args()

    print("=" * 50)
    print("BernCo Intel → GoHighLevel Push")
    print(f"Location: {GHL_LOCATION_ID}")
    print("=" * 50)

    # Resolve pipeline + stage IDs at startup (both modes need this)
    print("\nResolving pipeline…")
    if not resolve_pipeline():
        print("  ✗ Could not resolve pipeline — opportunities will be skipped")
        print("  Check PIPELINE_NAME / STAGE_NAME env vars or JustJarvis pipeline settings")
    print()

    # ── CSV mode ──────────────────────────────────────────────────────────
    if args.csv:
        push_from_csv(args.csv)
        return

    # ── records.json mode ─────────────────────────────────────────────────
    if not RECORDS_PATH.exists():
        print("No records.json found — skipping")
        sys.exit(0)

    data    = json.loads(RECORDS_PATH.read_text(encoding="utf-8"))
    records = data.get("records", [])

    hot = [
        r for r in records
        if r.get("score", 0) >= HOT_THRESHOLD
        and r.get("owner", "")
        and (
            not is_entity(r.get("owner", ""))
            or r.get("is_residential", False)
        )
    ]

    print(f"Total records : {len(records)}")
    print(f"Hot individuals (≥{HOT_THRESHOLD}): {len(hot)}")

    if not hot:
        print("No hot individual leads to push — done")
        return

    pushed = failed = 0
    for rec in hot:
        try:
            if push_contact(rec):
                pushed += 1
            else:
                failed += 1
        except Exception:
            print(f"  Error: {traceback.format_exc()}")
            failed += 1

    print("=" * 50)
    print(f"Pushed: {pushed}  |  Failed: {failed}  |  Total: {len(hot)}")
    print("Done ✓")


if __name__ == "__main__":
    main()
